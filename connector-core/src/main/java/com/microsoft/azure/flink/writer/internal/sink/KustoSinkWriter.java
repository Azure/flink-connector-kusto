package com.microsoft.azure.flink.writer.internal.sink;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Collector;
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
    this.kustoSinkCommon.close();
  }

  @VisibleForTesting
  void doBulkWrite() {
    if (bulkRequests.isEmpty()) {
      // no records to write
      LOG.warn("No records to write to DB {} & table {} ", writeOptions.getDatabase(),
          writeOptions.getTable());
      return;
    }
    LOG.debug("Ingesting to DB {} & table {} record count {}", writeOptions.getDatabase(),
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
      LOG.info("OverMaxBatchSizeLimit triggered at time {} with batch size {}.",
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
      LOG.info(
          "OverMaxBatchIntervalLimit triggered last sent interval data at {} against lastSentTime {}."
              + "The last sent interval is {}",
          Instant.now(Clock.systemUTC()), this.kustoSinkCommon.lastSendTime, lastSentInterval);
    }
    return isOverIntervalLimit;
  }
}
