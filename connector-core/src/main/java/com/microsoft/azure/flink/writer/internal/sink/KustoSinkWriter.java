package com.microsoft.azure.flink.writer.internal.sink;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Collector;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class KustoSinkWriter<IN> implements SinkWriter<IN> {
  private static final Logger LOG = LoggerFactory.getLogger(KustoSinkWriter.class);
  private final KustoSinkCommon<IN> kustoSinkCommon;
  private final KustoWriteOptions writeOptions;
  private final MailboxExecutor mailboxExecutor;
  private final boolean flushOnCheckpoint;
  private final List<IN> bulkRequests = new ArrayList<>();
  private final Collector<IN> collector;
  private final Counter numRecordsOut;
  private boolean checkpointInProgress = false;
  private transient volatile boolean closed = false;
  private transient ScheduledExecutorService scheduler;
  private transient ScheduledFuture<?> scheduledFuture;

  private transient volatile Exception flushException;

  public KustoSinkWriter(KustoConnectionOptions connectionOptions, KustoWriteOptions writeOptions,
      @NotNull TypeSerializer<IN> serializer, @NotNull TypeInformation<IN> typeInformation,
      boolean flushOnCheckpoint, Sink.InitContext initContext) throws URISyntaxException {
    this.writeOptions = checkNotNull(writeOptions);
    this.flushOnCheckpoint = flushOnCheckpoint;
    checkNotNull(initContext);
    this.mailboxExecutor = checkNotNull(initContext.getMailboxExecutor());
    SinkWriterMetricGroup metricGroup = checkNotNull(initContext.metricGroup());
    this.numRecordsOut = metricGroup.getNumRecordsSendCounter();
    this.collector = new ListCollector<>(this.bulkRequests);
    LOG.info("Initializing the class from KustoSinkWriter");
    this.kustoSinkCommon = new KustoSinkCommon<>(checkNotNull(connectionOptions), this.writeOptions,
        metricGroup, serializer, typeInformation, KustoSinkWriter.class.getSimpleName());
    // Bunch of metrics to send the data to monitor
    metricGroup.setCurrentSendTimeGauge(
        () -> this.kustoSinkCommon.ackTime - this.kustoSinkCommon.lastSendTime);

    boolean flushOnlyOnCheckpoint =
        writeOptions.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE;

    if (!flushOnlyOnCheckpoint && writeOptions.getBatchIntervalMs() > 0) {
      this.scheduler =
          Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("kusto-writer"));

      this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
        synchronized (KustoSinkWriter.this) {
          if (!closed && isOverMaxBatchIntervalLimit()) {
            try {
              doBulkWrite();
            } catch (Exception e) {
              flushException = e;
            }
          }
        }
      }, writeOptions.getBatchIntervalMs(), writeOptions.getBatchIntervalMs(),
          TimeUnit.MILLISECONDS);
    }


  }

  @Override
  public void write(IN element, Context context) throws IOException, InterruptedException {
    checkFlushException();
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
    checkFlushException();
    checkpointInProgress = true;
    while (!bulkRequests.isEmpty() && (flushOnCheckpoint || endOfInput)) {
      doBulkWrite();
    }
    checkpointInProgress = false;
  }

  @Override
  public synchronized void close() throws Exception {
    if (!closed) {
      if (scheduledFuture != null) {
        scheduledFuture.cancel(false);
        scheduler.shutdown();
      }

      if (!bulkRequests.isEmpty()) {
        try {
          doBulkWrite();
        } catch (Exception e) {
          LOG.error("Writing records to Kusto failed when closing MongoWriter", e);
          throw new IOException("Writing records to Kusto failed.", e);
        } finally {
          this.kustoSinkCommon.close();
          closed = true;
        }
      } else {
        this.kustoSinkCommon.close();
        closed = true;
      }
    }
  }

  void doBulkWrite() throws IOException {
    if (bulkRequests.isEmpty()) {
      // no records to write
      LOG.debug("No records to write to DB {} & table {} ", writeOptions.getDatabase(),
          writeOptions.getTable());
      return;
    }
    LOG.info("Ingesting to DB {} & table {} record count {}", writeOptions.getDatabase(),
        writeOptions.getTable(), bulkRequests.size());
    if (this.kustoSinkCommon.ingest(this.bulkRequests)) {
      // All the ingestion has completed successfully here. Clear this batch of records
      this.bulkRequests.clear();
    }
  }

  private boolean isOverMaxBatchSizeLimit() {
    long bulkActions = writeOptions.getBatchSize();
    boolean isOverMaxBatchSizeLimit = bulkActions != -1 && this.bulkRequests.size() >= bulkActions;
    if (isOverMaxBatchSizeLimit) {
      LOG.debug("OverMaxBatchSizeLimit triggered at time {} with batch size {}.",
          Instant.now(Clock.systemUTC()), this.bulkRequests.size());
    }
    return isOverMaxBatchSizeLimit;
  }

  private boolean isOverMaxBatchIntervalLimit() {
    long bulkFlushInterval = writeOptions.getBatchIntervalMs();
    long lastSentInterval =
        Instant.now(Clock.systemUTC()).toEpochMilli() - this.kustoSinkCommon.lastSendTime;
    boolean isOverIntervalLimit = this.kustoSinkCommon.lastSendTime > 0 && bulkFlushInterval != -1
        && lastSentInterval >= bulkFlushInterval;
    if (isOverIntervalLimit) {
      LOG.debug(
          "OverMaxBatchIntervalLimit triggered last sent interval data at {} against lastSentTime {}."
              + "The last sent interval is {}",
          Instant.now(Clock.systemUTC()), Instant.ofEpochMilli(this.kustoSinkCommon.lastSendTime),
          lastSentInterval);
    }
    return isOverIntervalLimit;
  }

  private void checkFlushException() {
    if (flushException != null) {
      throw new RuntimeException("Writing records to Kusto failed.", flushException);
    }
  }
}
