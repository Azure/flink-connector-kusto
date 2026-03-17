package com.microsoft.azure.flink.writer.internal.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.scala.typeutils.CaseClassSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.writer.internal.ContainerProvider;
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas;

import scala.Product;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sink writer for the two-phase commit protocol. Records are buffered in memory and uploaded to
 * Azure Blob Storage during {@link #prepareCommit()}, producing {@link KustoCommittable}s that the
 * {@link KustoSinkCommitter} will use to trigger Kusto ingestion.
 */
@Internal
public class KustoPrecommittingSinkWriter<IN>
    implements CommittingSinkWriter<IN, KustoCommittable> {
  private static final Logger LOG = LoggerFactory.getLogger(KustoPrecommittingSinkWriter.class);

  private final KustoConnectionOptions connectionOptions;
  private final KustoWriteOptions writeOptions;
  private final List<IN> bufferedRecords = new ArrayList<>();
  private final Supplier<Integer> aritySupplier;
  private final BiFunction<IN, Integer, Object> extractFieldValueFunction;
  private final Counter numRecordsOut;
  private volatile boolean closed = false;

  public KustoPrecommittingSinkWriter(KustoConnectionOptions connectionOptions,
      KustoWriteOptions writeOptions, TypeSerializer<IN> serializer,
      TypeInformation<IN> typeInformation, WriterInitContext initContext) {
    this.connectionOptions = checkNotNull(connectionOptions);
    this.writeOptions = checkNotNull(writeOptions);
    SinkWriterMetricGroup metricGroup = checkNotNull(initContext.metricGroup());
    this.numRecordsOut = metricGroup.getNumRecordsSendCounter();

    Class<?> clazzType = serializer.createInstance().getClass();
    if (Tuple.class.isAssignableFrom(clazzType)) {
      this.aritySupplier = () -> ((TupleSerializer<?>) serializer).getArity();
      this.extractFieldValueFunction = (value, index) -> ((Tuple) value).getField(index);
    } else if (Row.class.isAssignableFrom(clazzType)) {
      this.aritySupplier = () -> ((RowSerializer) serializer).getArity();
      this.extractFieldValueFunction = (value, index) -> ((Row) value).getField(index);
    } else if (Product.class.isAssignableFrom(clazzType)) {
      this.aritySupplier = () -> ((CaseClassSerializer<?>) serializer).getArity();
      this.extractFieldValueFunction = (value, index) -> ((Product) value).productElement(index);
    } else if (typeInformation instanceof PojoTypeInfo) {
      this.aritySupplier = typeInformation::getArity;
      this.extractFieldValueFunction = (value, index) -> {
        try {
          return ((PojoTypeInfo<IN>) typeInformation).getPojoFieldAt(index).getField().get(value);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      };
    } else {
      throw new IllegalArgumentException("Unsupported type: " + clazzType);
    }
  }

  @Override
  public void write(IN element, Context context) throws IOException {
    numRecordsOut.inc();
    bufferedRecords.add(element);
  }

  @Override
  public Collection<KustoCommittable> prepareCommit() throws IOException {
    if (bufferedRecords.isEmpty()) {
      return Collections.emptyList();
    }
    LOG.info("Preparing commit for {} records to DB {} table {}", bufferedRecords.size(),
        writeOptions.getDatabase(), writeOptions.getTable());

    List<KustoCommittable> committables = new ArrayList<>();
    ContainerProvider containerProvider =
        new ContainerProvider.Builder(this.connectionOptions).build();
    ContainerWithSas uploadContainer = containerProvider.getBlobContainer();
    BlobContainerClient blobContainerClient = new BlobContainerClientBuilder()
        .endpoint(uploadContainer.getEndpointWithoutSas())
        .sasToken(uploadContainer.getSas())
        .buildClient();
    UUID sourceId = UUID.randomUUID();
    AtomicInteger idx = new AtomicInteger(0);
    long recordCount = 0;

    try (BlobOutputMultiVolume compressedBlob =
        new BlobOutputMultiVolume(this.writeOptions.getClientBatchSizeLimit(), () -> {
          String blobName = String.format("%s-%s-%s-%s.csv.gz", writeOptions.getDatabase(),
              writeOptions.getTable(), sourceId, idx.incrementAndGet());
          LOG.debug("Writing to blob {}", blobName);
          OutputStream byteOut = blobContainerClient.getBlobClient(blobName).getBlockBlobClient()
              .getBlobOutputStream(true);
          return new GZIPOutputStream(byteOut);
        })) {
      int currentBlobIndex = idx.get();
      for (IN value : bufferedRecords) {
        int arity = this.aritySupplier.get();
        for (int i = 0; i < arity; i++) {
          Object fieldValue = this.extractFieldValueFunction.apply(value, i);
          if (fieldValue != null) {
            compressedBlob.write(StringEscapeUtils.escapeCsv(fieldValue.toString()).getBytes());
          }
          if (i < arity - 1) {
            compressedBlob.write(",".getBytes());
          }
        }
        compressedBlob.write(System.lineSeparator().getBytes());
        recordCount++;
        if (currentBlobIndex != idx.get()) {
          // Volume switched — emit committable for the completed blob
          String completedBlobName = String.format("%s-%s-%s-%s.csv.gz", writeOptions.getDatabase(),
              writeOptions.getTable(), sourceId, currentBlobIndex);
          committables.add(new KustoCommittable(uploadContainer.getEndpointWithoutSas(),
              uploadContainer.getSas(), completedBlobName, sourceId, recordCount));
          recordCount = 0;
          currentBlobIndex = idx.get();
        }
      }
    }

    if (recordCount > 0 || committables.isEmpty()) {
      // Emit committable for the final (or only) blob
      String finalBlobName = String.format("%s-%s-%s-%s.csv.gz", writeOptions.getDatabase(),
          writeOptions.getTable(), sourceId, idx.get());
      committables.add(new KustoCommittable(uploadContainer.getEndpointWithoutSas(),
          uploadContainer.getSas(), finalBlobName, sourceId, recordCount));
    }

    bufferedRecords.clear();
    LOG.info("Prepared {} committable(s) for ingestion", committables.size());
    return committables;
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    // No-op: all flushing happens in prepareCommit()
  }

  @Override
  public void close() throws Exception {
    closed = true;
    bufferedRecords.clear();
  }
}
