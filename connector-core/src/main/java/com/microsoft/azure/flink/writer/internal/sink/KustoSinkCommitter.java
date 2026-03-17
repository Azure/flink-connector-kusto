package com.microsoft.azure.flink.writer.internal.sink;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.common.KustoClientUtil;
import com.microsoft.azure.flink.common.KustoRetryConfig;
import com.microsoft.azure.flink.common.KustoRetryUtil;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.kusto.ingest.ColumnMapping;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Committer for the two-phase commit protocol. Receives {@link KustoCommittable}s containing blob
 * references and triggers Kusto ingestion from those blobs. Optionally polls for ingestion
 * completion.
 */
@Internal
public class KustoSinkCommitter implements Committer<KustoCommittable> {
  private static final Logger LOG = LoggerFactory.getLogger(KustoSinkCommitter.class);

  private static final int HISTOGRAM_WINDOW_SIZE = 1000;

  private final KustoConnectionOptions connectionOptions;
  private final KustoWriteOptions writeOptions;
  private final IngestClient ingestClient;
  private final IngestionMapping pojoIngestionMapping;
  private final ScheduledExecutorService pollExecutor =
      Executors.newSingleThreadScheduledExecutor();
  private final Counter ingestionSucceededCounter;
  private final Counter ingestionFailedCounter;
  private final Histogram ingestionLatencyHistogram;

  public KustoSinkCommitter(KustoConnectionOptions connectionOptions,
      KustoWriteOptions writeOptions, String[] pojoFieldNames,
      SinkCommitterMetricGroup metricGroup) throws URISyntaxException {
    this.connectionOptions = checkNotNull(connectionOptions);
    this.writeOptions = checkNotNull(writeOptions);
    this.ingestClient = KustoClientUtil.createIngestClient(connectionOptions,
        KustoSinkCommitter.class.getSimpleName());
    this.pojoIngestionMapping = (pojoFieldNames != null) ? createPojoMapping(pojoFieldNames) : null;
    this.ingestionSucceededCounter = metricGroup.counter("ingestionSucceeded");
    this.ingestionFailedCounter = metricGroup.counter("ingestionFailed");
    this.ingestionLatencyHistogram = metricGroup.histogram("ingestionLatencyMs",
        new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
  }

  private static IngestionMapping createPojoMapping(String[] pojoFields) {
    ColumnMapping[] columnMappings = new ColumnMapping[pojoFields.length];
    for (int i = 0; i < pojoFields.length; i++) {
      columnMappings[i] = new ColumnMapping(pojoFields[i], null);
      columnMappings[i].setOrdinal(i);
    }
    return new IngestionMapping(columnMappings, IngestionMapping.IngestionMappingKind.CSV);
  }

  @Override
  public void commit(Collection<CommitRequest<KustoCommittable>> commitRequests)
      throws IOException, InterruptedException {
    for (CommitRequest<KustoCommittable> request : commitRequests) {
      KustoCommittable committable = request.getCommittable();
      try {
        LOG.info("Committing blob {} with {} records to DB {} table {}", committable.getBlobName(),
            committable.getRecordCount(), writeOptions.getDatabase(), writeOptions.getTable());

        IngestionResult result = triggerIngestion(committable);

        if (writeOptions.getPollForIngestionStatus()) {
          pollForCompletion(committable, result, request);
        } else {
          LOG.debug("Ingestion triggered for blob {}. Polling disabled.",
              committable.getBlobName());
        }
      } catch (Exception e) {
        LOG.error("Failed to commit blob {}: {}", committable.getBlobName(), e.getMessage(), e);
        request.signalFailedWithUnknownReason(e);
      }
    }
  }

  private IngestionResult triggerIngestion(KustoCommittable committable) {
    return KustoRetryUtil.getRetries(KustoRetryConfig.builder().build()).executeSupplier(() -> {
      try {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(committable.getBlobUri());
        blobSourceInfo.setSourceId(committable.getSourceId());

        IngestionProperties ingestionProperties =
            new IngestionProperties(writeOptions.getDatabase(), writeOptions.getTable());
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
        ingestionProperties
            .setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
        ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV.name());
        ingestionProperties.setFlushImmediately(writeOptions.getFlushImmediately());

        if (!writeOptions.getIngestByTags().isEmpty()) {
          ingestionProperties.setIngestByTags(writeOptions.getIngestByTags());
        }
        if (!writeOptions.getAdditionalTags().isEmpty()) {
          ingestionProperties.setAdditionalTags(writeOptions.getAdditionalTags());
        }
        if (StringUtils.isNotEmpty(writeOptions.getIngestionMappingRef())) {
          ingestionProperties.setIngestionMapping(writeOptions.getIngestionMappingRef(),
              IngestionMapping.IngestionMappingKind.CSV);
        }
        if (pojoIngestionMapping != null) {
          ingestionProperties.setIngestionMapping(pojoIngestionMapping);
        }

        return ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
      } catch (IngestionClientException | IngestionServiceException e) {
        throw new RuntimeException(
            "Failed to trigger ingestion for blob: " + committable.getBlobName(), e);
      }
    });
  }

  private void pollForCompletion(KustoCommittable committable, IngestionResult ingestionResult,
      CommitRequest<KustoCommittable> request) {
    String sourceId = committable.getSourceId().toString();
    long ingestionStart = Instant.now(Clock.systemUTC()).toEpochMilli();
    CompletableFuture<String> completionFuture = new CompletableFuture<>();
    long timeToEndPoll =
        Instant.now(Clock.systemUTC()).toEpochMilli() + writeOptions.getPollTimeoutMs();
    long pollIntervalSeconds = Math.max(1, writeOptions.getPollIntervalMs() / 1000);

    final ScheduledFuture<?> checkFuture = pollExecutor.scheduleAtFixedRate(() -> {
      if (Instant.now(Clock.systemUTC()).toEpochMilli() > timeToEndPoll) {
        completionFuture.completeExceptionally(
            new TimeoutException("Polling for ingestion of source id: " + sourceId + " timed out"));
        return;
      }
      try {
        ingestionResult.getIngestionStatusCollection().stream()
            .filter(s -> s.getIngestionSourceId().toString().equals(sourceId)).findFirst()
            .ifPresent(status -> {
              if (status.status == OperationStatus.Succeeded) {
                long latencyMs =
                    Instant.now(Clock.systemUTC()).toEpochMilli() - ingestionStart;
                ingestionSucceededCounter.inc();
                ingestionLatencyHistogram.update(latencyMs);
                LOG.info("Ingestion succeeded for blob {} in {} ms", committable.getBlobName(),
                    latencyMs);
                completionFuture.complete(status.status.name());
              } else if (status.status == OperationStatus.Failed) {
                ingestionFailedCounter.inc();
                completionFuture
                    .completeExceptionally(new RuntimeException("Ingestion failed for blob "
                        + committable.getBlobName() + ": " + status.getFailureStatus()));
              } else if (status.status == OperationStatus.PartiallySucceeded) {
                LOG.warn("Ingestion partially succeeded for blob {}: {}", committable.getBlobName(),
                    status.getFailureStatus());
                completionFuture.complete(status.status.name());
              }
            });
      } catch (URISyntaxException e) {
        completionFuture.completeExceptionally(e);
      }
    }, 1, pollIntervalSeconds, TimeUnit.SECONDS);
    completionFuture.whenComplete((result, thrown) -> checkFuture.cancel(true));

    try {
      String result = completionFuture.get();
      if (OperationStatus.Failed.name().equals(result)) {
        request.signalFailedWithKnownReason(
            new RuntimeException("Ingestion failed for blob: " + committable.getBlobName()));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      request.retryLater();
    } catch (ExecutionException e) {
      LOG.error("Ingestion polling failed for blob {}", committable.getBlobName(), e.getCause());
      request.retryLater();
    }
  }

  @Override
  public void close() throws Exception {
    try {
      pollExecutor.shutdownNow();
    } catch (Exception e) {
      LOG.error("Error shutting down poll executor", e);
    }
    try {
      if (ingestClient != null) {
        ingestClient.close();
      }
    } catch (Exception e) {
      LOG.error("Error closing ingest client", e);
    }
  }
}
