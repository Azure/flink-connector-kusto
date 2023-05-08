package com.microsoft.azure.flink.writer.internal;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
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

@Internal
public class KustoSinkWriter<IN> implements SinkWriter<IN> {

  private static final Logger LOG = LoggerFactory.getLogger(KustoSinkWriter.class);

  private final KustoConnectionOptions connectionOptions;
  private final KustoWriteOptions writeOptions;
  private final MailboxExecutor mailboxExecutor;
  private final boolean flushOnCheckpoint;
  private final List<IN> bulkRequests = new ArrayList<>();
  private final Collector<IN> collector;
  private final Counter numRecordsOut;
  private final IngestClient ingestClient;


  private final transient Supplier<Integer> aritySupplier;
  private final transient BiFunction<IN, Integer, Object> extractFieldValueFunction;
  private final ScheduledExecutorService pollResultsExecutor =
      Executors.newSingleThreadScheduledExecutor();

  private final transient Counter recordsSent;
  private final transient Counter ingestSucceededCounter;
  private final transient Counter ingestFailedCounter;
  private final transient Counter ingestPartiallyFailedCounter;

  private boolean checkpointInProgress = false;
  private volatile long lastSendTime = 0L;
  private volatile long ackTime = Long.MAX_VALUE;

  public KustoSinkWriter(KustoConnectionOptions connectionOptions, KustoWriteOptions writeOptions,
      @NotNull TypeSerializer<IN> serializer, boolean flushOnCheckpoint,
      Sink.InitContext initContext) throws URISyntaxException {
    this.connectionOptions = checkNotNull(connectionOptions);
    this.writeOptions = checkNotNull(writeOptions);
    this.flushOnCheckpoint = flushOnCheckpoint;

    checkNotNull(initContext);
    this.mailboxExecutor = checkNotNull(initContext.getMailboxExecutor());

    SinkWriterMetricGroup metricGroup = checkNotNull(initContext.metricGroup());
    metricGroup.setCurrentSendTimeGauge(() -> ackTime - lastSendTime);

    this.numRecordsOut = metricGroup.getNumRecordsSendCounter();
    this.collector = new ListCollector<>(this.bulkRequests);
    Class<?> clazzType = serializer.createInstance().getClass();
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
    } else {
      throw new IllegalArgumentException("Unsupported type: " + clazzType);
    }
    this.ingestClient = KustoClientUtil.createIngestClient(checkNotNull(this.connectionOptions,
        "Connection options passed to ingest client cannot be null."));
    this.recordsSent = metricGroup.counter("recordsSent");
    this.ingestSucceededCounter = metricGroup.counter("succeededIngestions");
    this.ingestFailedCounter = metricGroup.counter("failedIngestions");
    this.ingestPartiallyFailedCounter = metricGroup.counter("partialSucceededIngestions");
  }

  @Override
  public void write(IN element, Context context) throws IOException, InterruptedException {
    // do not allow new bulk writes until all actions are flushed
    while (checkpointInProgress) {
      mailboxExecutor.yield();
    }
    numRecordsOut.inc();
    collector.collect(element);
    if (isOverMaxBatchSizeLimit() || isOverMaxBatchIntervalLimit()) {
      doBulkWrite();
    }
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    checkpointInProgress = true;
    while (!bulkRequests.isEmpty() && (flushOnCheckpoint || endOfInput)) {
      doBulkWrite();
    }
    checkpointInProgress = false;
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

  @VisibleForTesting
  void doBulkWrite() throws IOException {
    if (bulkRequests.isEmpty()) {
      // no records to write
      return;
    }
    UUID sourceId = UUID.randomUUID();
    String blobName = String.format("%s-%s-%s.csv.gz", this.writeOptions.getDatabase(),
        this.writeOptions.getTable(), sourceId);
    ContainerProvider containerProvider =
        new ContainerProvider.Builder(this.connectionOptions).build();
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
      this.lastSendTime = Instant.now(Clock.systemUTC()).toEpochMilli();
      for (IN value : this.bulkRequests) {
        for (int i = 0; i < this.aritySupplier.get(); i++) {
          Object fieldValue = this.extractFieldValueFunction.apply(value, i);
          if (fieldValue != null) {
            gzip.write(StringEscapeUtils.escapeCsv(fieldValue.toString()).getBytes());
          }
          if (i < this.aritySupplier.get() - 1) {
            gzip.write(",".getBytes());
          }
        }
        gzip.write(System.lineSeparator().getBytes());
        this.recordsSent.inc();
      }
      this.ackTime = System.currentTimeMillis();
      LOG.info("Records sent in blob {} with record count {}", blobName, this.bulkRequests.size());
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
        final String pollResult =
            pollForCompletion(blobName, sourceId.toString(), ingestionResult).get();
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
        LOG.debug("Ingesting into blob: {} with source id {}", blobUri, sourceId);
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
        return this.ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
      } catch (IngestionClientException | IngestionServiceException e) {
        String errorMessage = String
            .format("URI syntax exception polling ingestion status for sourceId: %s", sourceId);
        LOG.warn(errorMessage, e);
        throw new RuntimeException(errorMessage, e);
      }
    };
  }

  private @NotNull CompletableFuture<String> pollForCompletion(final String blobName,
      final String sourceId, IngestionResult ingestionResult) {
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
        LOG.debug("Ingestion Status {}",
            ingestionResult.getIngestionStatusCollection().stream()
                .map(is -> is.getIngestionSourceId() + "::" + is.getStatus())
                .collect(Collectors.joining(",")));
        ingestionResult.getIngestionStatusCollection().stream().filter(
            ingestionStatus -> ingestionStatus.getIngestionSourceId().toString().equals(sourceId))
            .findFirst().ifPresent(ingestionStatus -> {
              if (ingestionStatus.status == OperationStatus.Succeeded) {
                this.ingestSucceededCounter.inc();
                completionFuture.complete(ingestionStatus.status.name());
              } else if (ingestionStatus.status == OperationStatus.Failed) {
                this.ingestFailedCounter.inc();
                String failureReason = String.format(
                    "Ingestion failed for sourceId: %s with failure reason %s.Blob name %s",
                    sourceId, ingestionStatus.getFailureStatus(), blobName);
                LOG.error(failureReason);
                completionFuture.completeExceptionally(new RuntimeException(failureReason));
              } else if (ingestionStatus.status == OperationStatus.PartiallySucceeded) {
                // TODO check if this is really the case. What happens if one the blobs was
                // malformed ?
                this.ingestPartiallyFailedCounter.inc();
                String failureReason = String.format(
                    "Ingestion failed for sourceId: %s with failure reason %s. "
                        + "This will result in duplicates if the error was transient.Blob name: %s",
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
    }, 1, 5, TimeUnit.SECONDS); // TODO pick up from write options. Also CRP needs to be picked up
    completionFuture.whenComplete((result, thrown) -> checkFuture.cancel(true));
    return completionFuture;
  }

  private boolean isOverMaxBatchSizeLimit() {
    long bulkActions = writeOptions.getBatchSize();
    boolean isOverMaxBatchSizeLimit = bulkActions != -1 && this.bulkRequests.size() >= bulkActions;
    if (isOverMaxBatchSizeLimit) {
      LOG.info("OverMaxBatchSizeLimit triggered at time {} with batch size {}.", Instant.now(),
          this.bulkRequests.size());
    }
    return isOverMaxBatchSizeLimit;
  }

  private boolean isOverMaxBatchIntervalLimit() {
    long bulkFlushInterval = writeOptions.getBatchIntervalMs();
    long lastSentInterval = System.currentTimeMillis() - lastSendTime;
    boolean isOverIntervalLimit =
        lastSendTime > 0 && bulkFlushInterval != -1 && lastSentInterval >= bulkFlushInterval;
    if (isOverIntervalLimit) {
      LOG.info(
          "OverMaxBatchIntervalLimit triggered last sent interval data at {} against lastSentTime {}."
              + "The last sent interval is {}",
          Instant.now(), this.lastSendTime, lastSentInterval);
    }
    return isOverIntervalLimit;
  }
}
