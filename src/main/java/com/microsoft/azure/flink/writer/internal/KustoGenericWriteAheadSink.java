package com.microsoft.azure.flink.writer.internal;

import java.io.IOException;
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
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.scala.typeutils.CaseClassSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.apache.flink.types.Row;
import org.jetbrains.annotations.NotNull;

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

@PublicEvolving
@Internal
public class KustoGenericWriteAheadSink<IN> extends GenericWriteAheadSink<IN> {
  private final KustoConnectionOptions connectionOptions;
  private final KustoWriteOptions writeOptions;

  private IngestClient ingestClient;

  private final ScheduledExecutorService pollResultsExecutor =
      Executors.newSingleThreadScheduledExecutor();

  // Bunch of metrics to send the data to monitor
  private transient Counter recordsSent;
  private transient Counter ingestSucceededCounter;
  private transient Counter ingestFailedCounter;
  private transient Counter ingestPartiallyFailedCounter;

  private final transient Supplier<Integer> aritySupplier;
  private final transient BiFunction<IN, Integer, Object> extractFieldValueFunction;

  /**
   * Constructor for KustoGenericWriteAheadSink.
   *
   * @param connectionOptions The connection options for the Kusto cluster.
   * @param writeOptions The write options for the Kusto cluster.
   * @param committer The committer for the checkpoint.
   * @param serializer The serializer for the data.
   * @param jobID The job ID.
   * @throws Exception If the connection options are invalid.
   */
  public KustoGenericWriteAheadSink(KustoConnectionOptions connectionOptions,
      KustoWriteOptions writeOptions, CheckpointCommitter committer, TypeSerializer<IN> serializer,
      String jobID) throws Exception {
    super(committer, serializer, jobID);
    this.connectionOptions = connectionOptions;
    this.writeOptions = writeOptions;
    Class<?> clazzType = serializer.createInstance().getClass();
    LOG.info("Using WriteAheadSink class type: {}", clazzType);
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
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!getRuntimeContext().isCheckpointingEnabled()) {
      throw new IllegalStateException("The write-ahead log requires checkpointing to be enabled.");
    }
    this.ingestClient = KustoClientUtil.createIngestClient(checkNotNull(this.connectionOptions,
        "Connection options passed to ingest client cannot be null."));

    this.recordsSent = super.getRuntimeContext().getMetricGroup().counter("recordsSent");
    this.ingestSucceededCounter =
        super.getRuntimeContext().getMetricGroup().counter("succeededIngestions");
    this.ingestFailedCounter =
        super.getRuntimeContext().getMetricGroup().counter("failedIngestions");
    this.ingestPartiallyFailedCounter =
        super.getRuntimeContext().getMetricGroup().counter("partialSucceededIngestions");
  }

  @Override
  protected boolean sendValues(@NotNull Iterable<IN> values, long checkpointId, long timestamp) {
    // Get the blob
    // Write to the blob
    // have the ingest client send out request for ingestion
    // wait for result of the ingest and return true/false
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
      for (IN value : values) {
        for (int i = 0; i < aritySupplier.get(); i++) {
          Object fieldValue = extractFieldValueFunction.apply(value, i);
          if (fieldValue != null) {
            gzip.write(StringEscapeUtils.escapeCsv(fieldValue.toString()).getBytes());
          }
          if (i < aritySupplier.get() - 1) {
            gzip.write(",".getBytes());
          }
        }
        gzip.write(System.lineSeparator().getBytes());
        this.recordsSent.inc();
      }
    } catch (IOException e) {
      LOG.error("Error (IOException) while writing to blob.", e);
      isUploadSuccessful = false;
    }
    if (isUploadSuccessful) {
      LOG.debug("Upload to blob successful , blob file {}.Performing ingestion", blobName);
      IngestionResult ingestionResult =
          KustoRetryUtil.getRetries(KustoRetryConfig.builder().build())
              .executeSupplier(performIngestSupplier(uploadContainerSas, blobName, sourceId));
      try {
        final String pollResult =
            pollForCompletion(blobName, sourceId.toString(), ingestionResult).get();
        return OperationStatus.Succeeded.name().equals(pollResult);
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error while polling for completion of ingestion.", e);
        throw new RuntimeException(e);
      }
    }
    return false;
  }

  private Supplier<IngestionResult> performIngestSupplier(@NotNull ContainerSas container,
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

  // https://stackoverflow.com/questions/40251528/how-to-use-executorservice-to-poll-until-a-result-arrives
  private CompletableFuture<String> pollForCompletion(final String blobName, final String sourceId,
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
        LOG.debug("Ingestion Status {} for blob {}",
            ingestionResult.getIngestionStatusCollection().stream()
                .map(is -> is.getIngestionSourceId() + "::" + is.getStatus())
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

  @Override
  public void close() throws Exception {
    super.close();
    try {
      if (ingestClient != null) {
        ingestClient.close();
      }
    } catch (Exception e) {
      LOG.error("Error while closing session.", e);
    }
  }
}
