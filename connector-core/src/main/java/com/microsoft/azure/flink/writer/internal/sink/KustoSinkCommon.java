package com.microsoft.azure.flink.writer.internal.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.scala.typeutils.CaseClassSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.microsoft.azure.flink.common.KustoClientUtil;
import com.microsoft.azure.flink.common.KustoRetryConfig;
import com.microsoft.azure.flink.common.KustoRetryUtil;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.writer.internal.ContainerProvider;
import com.microsoft.azure.flink.writer.internal.container.ContainerSas;
import com.microsoft.azure.kusto.ingest.ColumnMapping;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;

import scala.Product;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class KustoSinkCommon<IN> {
  private final KustoWriteOptions writeOptions;
  protected static final Logger LOG = LoggerFactory.getLogger(KustoSinkCommon.class);
  private final transient Counter ingestSucceededCounter;
  private final transient Counter ingestFailedCounter;
  private final transient Counter ingestPartiallyFailedCounter;
  private final transient Counter recordsSent;
  private final ScheduledExecutorService pollResultsExecutor =
      Executors.newSingleThreadScheduledExecutor();
  private transient final IngestClient ingestClient;
  private final KustoConnectionOptions connectionOptions;
  protected final transient Supplier<Integer> aritySupplier;
  protected final transient BiFunction<IN, Integer, Object> extractFieldValueFunction;
  protected IngestionMapping ingestionMapping;
  protected volatile long lastSendTime = 0L;
  protected volatile long ackTime = Long.MAX_VALUE;


  protected KustoSinkCommon(KustoConnectionOptions connectionOptions,
      KustoWriteOptions writeOptions, MetricGroup metricGroup, TypeSerializer<IN> serializer,
      TypeInformation<IN> typeInformation, String sourceClass) throws URISyntaxException {
    this.connectionOptions = connectionOptions;
    this.writeOptions = writeOptions;
    this.ingestClient = KustoClientUtil.createIngestClient(checkNotNull(connectionOptions,
        "Connection options passed to ingest client cannot be null."), sourceClass);
    this.ingestSucceededCounter = metricGroup.counter("succeededIngestions");
    this.ingestFailedCounter = metricGroup.counter("failedIngestions");
    this.ingestPartiallyFailedCounter = metricGroup.counter("partialSucceededIngestions");
    this.recordsSent = metricGroup.counter("recordsSent");
    Class<?> clazzType = serializer.createInstance().getClass();
    LOG.trace("Using sink with class type: {}", clazzType);
    if (Tuple.class.isAssignableFrom(clazzType)) {
      this.aritySupplier = () -> (((TupleSerializer<?>) serializer)).getArity();
      this.extractFieldValueFunction = (value, index) -> {
        Tuple tuple = (Tuple) value;
        return tuple.getField(index);
      };
    } else if (Row.class.isAssignableFrom(clazzType)) {
      this.aritySupplier = () -> (((RowSerializer) serializer)).getArity();
      this.extractFieldValueFunction = (value, index) -> {
        Row row = (Row) value;
        return row.getField(index);
      };
    } else if (Product.class.isAssignableFrom(clazzType)) {
      this.aritySupplier = () -> (((CaseClassSerializer<?>) serializer)).getArity();
      this.extractFieldValueFunction = (value, index) -> {
        Product product = (Product) value;
        return product.productElement(index);
      };
    } else if (typeInformation instanceof PojoTypeInfo) {
      this.aritySupplier = typeInformation::getArity;
      this.extractFieldValueFunction = (value, index) -> {
        try {
          return ((PojoTypeInfo<IN>) typeInformation).getPojoFieldAt(index).getField().get(value);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      };
      this.ingestionMapping =
          getIngestionMapping(((PojoTypeInfo<IN>) typeInformation).getFieldNames());
    } else {
      throw new IllegalArgumentException("Unsupported type: " + clazzType);
    }
  }

  /**
   * Given a set of fields, returns the ingestion mapping.
   *
   * @param pojoFields The fields in the pojo
   * @return The ingestion mapping
   */
  @Contract("_ -> new")
  private @NotNull IngestionMapping getIngestionMapping(String @NotNull [] pojoFields) {
    ColumnMapping[] columnMappings = new ColumnMapping[pojoFields.length];
    for (int fieldCount = 0; fieldCount < pojoFields.length; fieldCount++) {
      columnMappings[fieldCount] = new ColumnMapping(pojoFields[fieldCount], null);
      columnMappings[fieldCount].setOrdinal(fieldCount);
    }
    return new IngestionMapping(columnMappings, IngestionMapping.IngestionMappingKind.CSV);
  }

  @Contract(pure = true)
  @NotNull
  protected Supplier<IngestionResult> performIngestSupplier(@NotNull ContainerSas container,
      IngestionMapping ingestionMapping, @NotNull String blobName, UUID sourceId) {
    return () -> {
      try {
        String blobUri = String.format("%s/%s?%s", container.getContainerUrl(), blobName,
            container.getSasToken());
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobUri);
        LOG.trace("Ingesting into blob: {} with source id {}", blobUri, sourceId);
        blobSourceInfo.setSourceId(sourceId);
        IngestionProperties ingestionProperties =
            new IngestionProperties(this.writeOptions.getDatabase(), this.writeOptions.getTable());
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
        if (!this.writeOptions.getIngestByTags().isEmpty()) {
          ingestionProperties.setIngestByTags(this.writeOptions.getIngestByTags());
        }
        if (!this.writeOptions.getAdditionalTags().isEmpty()) {
          ingestionProperties.setAdditionalTags(this.writeOptions.getAdditionalTags());
        }
        ingestionProperties.setIngestByTags(this.writeOptions.getIngestByTags());
        ingestionProperties
            .setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV.name());
        if (StringUtils.isNotEmpty(this.writeOptions.getIngestionMappingRef())) {
          ingestionProperties.setIngestionMapping(this.writeOptions.getIngestionMappingRef(),
              IngestionMapping.IngestionMappingKind.CSV);
        }
        // For the POJO mapping
        if (ingestionMapping != null) {
          ingestionProperties.setIngestionMapping(ingestionMapping);
        }
        ingestionProperties.setFlushImmediately(this.writeOptions.getFlushImmediately());
        // This is when the last upload and queue request happened
        this.lastSendTime = Instant.now(Clock.systemUTC()).toEpochMilli();
        LOG.trace("Setting last send time to {}", this.lastSendTime);
        return this.ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
      } catch (IngestionClientException | IngestionServiceException e) {
        String errorMessage = String
            .format("URI syntax exception polling ingestion status for sourceId: %s", sourceId);
        LOG.warn(errorMessage, e);
        throw new RuntimeException(errorMessage, e);
      }
    };
  }


  boolean ingest(Iterable<IN> bulkRequests) {
    // Get the blob
    // Write to the blob
    // have the ingest client send out request for ingestion
    // wait for result of the ingest and return true/false
    if (bulkRequests == null) {
      return true;
    }
    ContainerProvider containerProvider =
        new ContainerProvider.Builder(this.connectionOptions).build();
    ContainerSas uploadContainerSas = containerProvider.getBlobContainer();
    String sasConnectionString = uploadContainerSas.toString();
    BlobContainerClient blobContainerClient =
        new BlobContainerClientBuilder().endpoint(sasConnectionString).buildClient();

    UUID sourceId = UUID.randomUUID();
    boolean isUploadSuccessful = true;
    // Is a side effect. Can be a bit more polished, it is easier to send the total metric in one
    // go.
    int recordsInBatch = 0;
    AtomicInteger idx = new AtomicInteger(0);
    try (BlobOutputMultiVolume compressedBlob =
        new BlobOutputMultiVolume(this.writeOptions.getClientBatchSizeLimit(), () -> {
          String blobNameWithIndex =
              String.format("%s-%s-%s-%s.csv.gz", this.writeOptions.getDatabase(),
                  this.writeOptions.getTable(), sourceId, idx.incrementAndGet());
          LOG.debug("Ingesting to blob {} to database {} and table {} and switching to blob {}",
              blobNameWithIndex, writeOptions.getDatabase(), writeOptions.getTable(),
              blobNameWithIndex);
          OutputStream byteOut = blobContainerClient.getBlobClient(blobNameWithIndex)
              .getBlockBlobClient().getBlobOutputStream(true);
          return new GZIPOutputStream(byteOut);
        })) {
      int currentBlobIndex = idx.get();
      for (IN value : bulkRequests) {
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
        recordsInBatch++;
        if (currentBlobIndex != idx.get()) {
          LOG.info("Switching to blob {} from blob {} to database {} and table {}", idx.get(),
              currentBlobIndex, writeOptions.getDatabase(), writeOptions.getTable());
          String blobNameWithIndex =
              String.format("%s-%s-%s-%s.csv.gz", this.writeOptions.getDatabase(),
                  this.writeOptions.getTable(), sourceId, currentBlobIndex);
          if (!uploadAndPollStatus(uploadContainerSas, sourceId, blobNameWithIndex)) {
            LOG.error(
                "Writing to database {} and table {} failed. SourceId {} and BlobId {} failed",
                writeOptions.getDatabase(), writeOptions.getTable(), sourceId, blobNameWithIndex);
            return false;
          }
          currentBlobIndex = idx.get();
        }
      }
      // If there are no records in the batch, we will mark this as done.
      if (recordsInBatch == 0) {
        return true;
      }
      this.recordsSent.inc(recordsInBatch);
    } catch (IOException e) {
      LOG.error("Error (IOException) while writing to blob.", e);
      isUploadSuccessful = false;
    }
    if (isUploadSuccessful) {
      String finalBlobName = String.format("%s-%s-%s-%s.csv.gz", this.writeOptions.getDatabase(),
          this.writeOptions.getTable(), sourceId, idx.get());
      LOG.info(
          "Flushing the final block of records to blob {} to ingest to database {} and table {} ",
          finalBlobName, writeOptions.getDatabase(), writeOptions.getTable());
      return uploadAndPollStatus(uploadContainerSas, sourceId, finalBlobName);
    }
    return false;
  }

  private boolean uploadAndPollStatus(ContainerSas uploadContainerSas, UUID sourceId,
      String blobName) {
    LOG.debug("Upload to blob successful , blob file {}.Performing ingestion", blobName);
    IngestionResult ingestionResult =
        KustoRetryUtil.getRetries(KustoRetryConfig.builder().build()).executeSupplier(this
            .performIngestSupplier(uploadContainerSas, this.ingestionMapping, blobName, sourceId));
    try {
      final String pollResult =
          this.pollForCompletion(blobName, sourceId.toString(), ingestionResult).get();
      this.ackTime = Instant.now(Clock.systemUTC()).toEpochMilli();
      return OperationStatus.Succeeded.name().equals(pollResult);
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error while polling for completion of ingestion.", e);
      throw new RuntimeException(e);
    }
  }

  protected CompletableFuture<String> pollForCompletion(String blobName, String sourceId,
      IngestionResult ingestionResult) {
    CompletableFuture<String> completionFuture = new CompletableFuture<>();
    // TODO: Should this be configurable?
    long timeToEndPoll = Instant.now(Clock.systemUTC()).plus(5, ChronoUnit.MINUTES).toEpochMilli();
    final ScheduledFuture<?> checkFuture = pollResultsExecutor.scheduleAtFixedRate(() -> {
      if (Instant.now(Clock.systemUTC()).toEpochMilli() > timeToEndPoll) {
        LOG.warn(
            "Time for polling end is {} and the current epoch time is {}. This operation will timeout",
            Instant.ofEpochMilli(timeToEndPoll), Instant.now(Clock.systemUTC()));
        completionFuture.completeExceptionally(new TimeoutException(
            "Polling for ingestion of source id: " + sourceId + " timed out."));
      }
      try {
        LOG.debug("Ingestion Status {} for blob {}",
            ingestionResult.getIngestionStatusCollection().stream()
                .map(is -> is.getIngestionSourceId() + ":" + is.getStatus())
                .collect(Collectors.joining(",")),
            blobName);
        ingestionResult.getIngestionStatusCollection().stream().filter(
            ingestionStatus -> ingestionStatus.getIngestionSourceId().toString().equals(sourceId))
            .findFirst().ifPresent(ingestionStatus -> {
              if (ingestionStatus.status == OperationStatus.Succeeded) {
                this.ingestSucceededCounter.inc();
                completionFuture.complete(ingestionStatus.status.name());
              } else if (ingestionStatus.status == OperationStatus.Failed) {
                this.ingestFailedCounter.inc();
                String failureReason = String.format(
                    "Ingestion failed for sourceId: %s with failure reason %s.Blob name : %s",
                    sourceId, ingestionStatus.getFailureStatus(), blobName);
                LOG.error(failureReason);
                completionFuture.completeExceptionally(new RuntimeException(failureReason));
              } else if (ingestionStatus.status == OperationStatus.PartiallySucceeded) {
                // TODO check if this is really the case. What happens if one the blobs was
                // malformed ?
                this.ingestPartiallyFailedCounter.inc();
                String failureReason = String.format(
                    "Ingestion failed for sourceId: %s with failure reason %s. "
                        + "This will result in duplicates if the error was transient and was retried.Blob name: %s",
                    sourceId, ingestionStatus.getFailureStatus(), blobName);
                LOG.warn(failureReason);
                completionFuture.complete(ingestionStatus.status.name());
              }
            });
      } catch (URISyntaxException e) {
        String errorMessage = String
            .format("URI syntax exception polling ingestion status for sourceId: %s", sourceId);
        LOG.warn(errorMessage, e);
        completionFuture.completeExceptionally(new RuntimeException(errorMessage, e));
      }
    }, 1, 5, TimeUnit.SECONDS); // TODO pick up from write options. Also CRP needs to be picked
    // up
    completionFuture.whenComplete((result, thrown) -> checkFuture.cancel(true));
    return completionFuture;
  }

  protected void close() {
    try {
      if (ingestClient != null) {
        ingestClient.close();
      }
    } catch (Exception e) {
      LOG.error("Error while closing session.", e);
    }
  }
}
