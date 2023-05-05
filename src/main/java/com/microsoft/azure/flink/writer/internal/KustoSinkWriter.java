package com.microsoft.azure.flink.writer.internal;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.scala.typeutils.CaseClassSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.microsoft.azure.flink.common.KustoClientUtil;
import com.microsoft.azure.flink.common.KustoRetryConfig;
import com.microsoft.azure.flink.common.KustoRetryUtil;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.writer.internal.container.ContainerSas;
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

public class KustoSinkWriter<IN> implements SinkWriter<IN> {
  protected static final Logger LOG = LoggerFactory.getLogger(KustoSinkWriter.class);
  private final KustoConnectionOptions connectionOptions;
  private final KustoWriteOptions writeOptions;
  private final boolean flushOnCheckpoint;
  private boolean checkpointInProgress = false;
  private final MailboxExecutor mailboxExecutor;
  private IngestClient ingestClient;
  private transient Object[] fields;
  private final transient Class<?> clazzType;
  private final TypeSerializer<IN> serializer;
  private volatile long ackTime = Long.MAX_VALUE;
  private volatile long lastSendTime = 0L;
  private final Counter numRecordsOut;
  private final List<IN> bulkRequests = new ArrayList<>();
  private final Collector<IN> collector;
  private final ScheduledExecutorService pollResultsExecutor =
      Executors.newSingleThreadScheduledExecutor();


  public KustoSinkWriter(KustoConnectionOptions connectionOptions, KustoWriteOptions writeOptions,
      @NotNull TypeSerializer<IN> serializer, boolean flushOnCheckpoint,
      Sink.InitContext initContext) throws Exception {
    this.connectionOptions = connectionOptions;
    this.writeOptions = writeOptions;
    // Use a CaseClass or Tuple or Row to ingest data
    this.serializer = serializer;
    this.clazzType = serializer.createInstance().getClass();
    this.flushOnCheckpoint = flushOnCheckpoint;
    checkNotNull(initContext);
    this.mailboxExecutor = checkNotNull(initContext.getMailboxExecutor());
    SinkWriterMetricGroup metricGroup = checkNotNull(initContext.metricGroup());
    metricGroup.setCurrentSendTimeGauge(() -> ackTime - lastSendTime);
    this.numRecordsOut = metricGroup.getNumRecordsSendCounter();
    this.collector = new ListCollector<>(this.bulkRequests);
    this.open();
  }

  public void open() throws Exception {
    this.ingestClient = KustoClientUtil.createIngestClient(checkNotNull(this.connectionOptions,
        "Connection options passed to ingest client cannot be null."));
    if (Tuple.class.isAssignableFrom(clazzType)) {
      int arity = (((TupleSerializer<?>) this.serializer)).getArity();
      this.fields = new Object[arity];
    } else if (Row.class.isAssignableFrom(clazzType)) {
      int arity = ((RowSerializer) this.serializer).getArity();
      this.fields = new Object[arity];
    } else if (Product.class.isAssignableFrom(clazzType)) {
      int arity = ((CaseClassSerializer<?>) this.serializer).getArity();
      this.fields = new Object[arity];
    }
  }

  protected void bulkWrite() {
    // Get the blob
    // Write to the blob
    // have the ingest client send out request for ingestion
    // wait for result of the ingest and return true/false
    UUID sourceId = UUID.randomUUID();
    String blobName = String.format("%s-%s-%s.csv.gz", this.writeOptions.getDatabase(),
        this.writeOptions.getTable(), sourceId);
    ContainerProvider containerProvider = new ContainerProvider(this.connectionOptions);
    ContainerSas uploadContainerSas = containerProvider.getBlobContainer();
    String sasConnectionString = uploadContainerSas.toString();
    BlobContainerClient blobContainerClient =
        new BlobContainerClientBuilder().endpoint(sasConnectionString).buildClient();
    boolean isUploadSuccessful = true;
    BlobClient uploadClient = blobContainerClient.getBlobClient(blobName);
    try (
        BlobOutputStream blobOutputStream =
            uploadClient.getBlockBlobClient().getBlobOutputStream(true);
        GZIPOutputStream gzip = new GZIPOutputStream(blobOutputStream)) {
      for (IN value : this.bulkRequests) {
        if (Tuple.class.isAssignableFrom(this.clazzType)) {
          Tuple tupleValue = (Tuple) value;
          for (int x = 0; x < tupleValue.getArity(); x++) {
            try {
              fields[x] = tupleValue.getField(x);
              if (!Objects.isNull(fields[x])) {
                gzip.write(StringEscapeUtils.escapeCsv(fields[x].toString())
                    .getBytes(StandardCharsets.UTF_8));
              }
              gzip.write(',');
            } catch (IOException e) {
              LOG.error("Error while writing row to blob using TupleValue.", e);
              isUploadSuccessful = false;
            }
          }
        } else if (Row.class.isAssignableFrom(this.clazzType)) {
          Row rowValue = (Row) value;
          for (int x = 0; x < rowValue.getArity(); x++) {
            try {
              fields[x] = rowValue.getField(x);
              if (!Objects.isNull(fields[x])) {
                gzip.write(StringEscapeUtils.escapeCsv(fields[x].toString())
                    .getBytes(StandardCharsets.UTF_8));
              }
              gzip.write(',');
            } catch (IOException e) {
              LOG.error("Error while writing row to blob using RowValue.", e);
              isUploadSuccessful = false;
            }
          }
        } else if (Product.class.isAssignableFrom(this.clazzType)) {
          Product product = (Product) value;
          for (int x = 0; x < product.productArity(); x++) {
            try {
              fields[x] = product.productElement(x);
              if (!Objects.isNull(fields[x])) {
                gzip.write(StringEscapeUtils.escapeCsv(fields[x].toString())
                    .getBytes(StandardCharsets.UTF_8));
              }
              gzip.write(',');
            } catch (IOException e) {
              LOG.error("Error while writing row to blob using RowValue.", e);
              isUploadSuccessful = false;
            }
          }
        }
        gzip.write(System.lineSeparator().getBytes());
      }
      this.bulkRequests.clear();
    } catch (IOException e) {
      LOG.error("Error (IOException) while writing to blob.", e);
      isUploadSuccessful = false;
    }
    if (isUploadSuccessful) {
      // The process is completed, so we can clear the bulkRequests
      IngestionResult ingestionResult =
          KustoRetryUtil.getRetries(KustoRetryConfig.builder().build())
              .executeSupplier(performIngestSupplier(uploadContainerSas, blobName, sourceId));
      try {
        final String pollResult = pollForCompletion(sourceId.toString(), ingestionResult).get();
        if (OperationStatus.Succeeded.name().equals(pollResult)) {
          LOG.info("Source {} successfully ingested into Kusto. Operation status: {}", sourceId,
              pollResult);
        } else {
          LOG.info("Source {} failed ingestion into Kusto. Operation status: {}", sourceId,
              pollResult);
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error while polling for completion of ingestion.", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Contract(pure = true)
  private @NotNull Supplier<IngestionResult> performIngestSupplier(@NotNull ContainerSas container,
      @NotNull String blobName, UUID sourceId) {
    return () -> {
      try {
        String blobUri = String.format("%s/%s?%s", container.getContainerUrl(), blobName,
            container.getSasToken());
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobUri);
        LOG.error("Ingesting into blob: {} with source id {}", blobUri, sourceId);
        blobSourceInfo.setSourceId(sourceId);
        IngestionProperties ingestionProperties =
            new IngestionProperties(this.writeOptions.getDatabase(), this.writeOptions.getTable());
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
        ingestionProperties
            .setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV.name());
        if (StringUtils.isNotEmpty(this.writeOptions.getIngestionMappingRef())) {
          ingestionProperties.setIngestionMapping(this.writeOptions.getIngestionMappingRef(),
              IngestionMapping.IngestionMappingKind.CSV);
        }
        ingestionProperties.setFlushImmediately(this.writeOptions.getFlushImmediately());
        this.lastSendTime = Instant.now(Clock.systemUTC()).toEpochMilli();
        return this.ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
      } catch (IngestionClientException | IngestionServiceException e) {
        String errorMessage = String
            .format("URI syntax exception polling ingestion status for sourceId: %s", sourceId);
        LOG.warn(errorMessage, e);
        throw new RuntimeException(errorMessage, e);
      }
    };
  }

  private CompletableFuture<String> pollForCompletion(final String sourceId,
      IngestionResult ingestionResult) {
    CompletableFuture<String> completionFuture = new CompletableFuture<>();
    long timeToEndPoll = Instant.now(Clock.systemUTC()).plus(5, ChronoUnit.MINUTES).toEpochMilli(); // TODO:
    // make
    // this
    // configurable
    final ScheduledFuture<?> checkFuture = pollResultsExecutor.scheduleAtFixedRate(() -> {
      if (Instant.now(Clock.systemUTC()).toEpochMilli() > timeToEndPoll) {
        LOG.warn(
            "Time for polling end is {} and the current epoch time is {}. This operation will timeout",
            Instant.ofEpochMilli(timeToEndPoll), Instant.now(Clock.systemUTC()));
        completionFuture.completeExceptionally(new TimeoutException(
            "Polling for ingestion of source id: " + sourceId + " timed out."));
      }
      try {
        LOG.error("Ingestion Status {}",
            ingestionResult.getIngestionStatusCollection().stream()
                .map(is -> is.getIngestionSourceId() + "::" + is.getStatus())
                .collect(Collectors.joining(",")));
        ingestionResult.getIngestionStatusCollection().stream().filter(
            ingestionStatus -> ingestionStatus.getIngestionSourceId().toString().equals(sourceId))
            .findFirst().ifPresent(ingestionStatus -> {
              if (ingestionStatus.status == OperationStatus.Succeeded) {
                completionFuture.complete(ingestionStatus.status.name());
              } else if (ingestionStatus.status == OperationStatus.Failed) {
                String failureReason =
                    String.format("Ingestion failed for sourceId: %s with failure reason %s",
                        sourceId, ingestionStatus.getFailureStatus());
                LOG.error(failureReason);
                completionFuture.completeExceptionally(new RuntimeException(failureReason));
              } else if (ingestionStatus.status == OperationStatus.PartiallySucceeded) {
                // TODO check if this is really the case. What happens if one the blobs was
                // malformed ?
                String failureReason = String.format(
                    "Ingestion failed for sourceId: %s with failure reason %s. "
                        + "This will result in duplicates if the error was transient.",
                    sourceId, ingestionStatus.getFailureStatus());
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
    }, 1, 5, TimeUnit.SECONDS); // TODO pick up from write options. Also CRP needs to be picked up
    completionFuture.whenComplete((result, thrown) -> checkFuture.cancel(true));
    this.ackTime = Instant.now(Clock.systemUTC()).toEpochMilli();
    return completionFuture;
  }

  @Override
  public void close() {
    try {
      if (ingestClient != null) {
        ingestClient.close();
      }
    } catch (Exception e) {
      LOG.error("Error while closing session.", e);
    }
  }

  @Override
  public void write(IN in, Context context) throws IOException, InterruptedException {
    // do not allow new bulk writes until all actions are flushed
    while (this.checkpointInProgress) {
      this.mailboxExecutor.yield();
    }
    this.numRecordsOut.inc();
    this.collector.collect(in);
    if (isOverMaxBatchSizeLimit() || isOverMaxBatchIntervalLimit()) {
      LOG.error("Bulk write limit reached. Performing bulk write. Record count {}",
          this.bulkRequests.size());
      bulkWrite();
    }
  }

  @Override
  public void flush(boolean endOfInput) throws IOException, InterruptedException {
    this.checkpointInProgress = true;
    while (!bulkRequests.isEmpty() && (this.flushOnCheckpoint || endOfInput)) {
      LOG.error("Bulk write time limit reached or batch size, flushing records. Record count {}",
          this.bulkRequests.size());
      bulkWrite();
    }
    this.checkpointInProgress = false;
  }

  private boolean isOverMaxBatchSizeLimit() {
    long bulkActions = this.writeOptions.getBatchSize();
    return bulkActions != -1 && this.bulkRequests.size() >= bulkActions;
  }

  private boolean isOverMaxBatchIntervalLimit() {
    long bulkFlushInterval = this.writeOptions.getBatchIntervalMs();
    long lastSentInterval = Instant.now(Clock.systemUTC()).toEpochMilli() - this.lastSendTime;
    return bulkFlushInterval != -1 && lastSentInterval >= bulkFlushInterval;
  }
}
