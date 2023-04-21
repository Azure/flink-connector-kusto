package com.microsoft.azure.flink.writer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.writer.context.DefaultKustoSinkContext;
import com.microsoft.azure.flink.writer.context.KustoSinkContext;
import com.microsoft.azure.flink.writer.serializer.KustoRow;
import com.microsoft.azure.flink.writer.serializer.KustoSerializationSchema;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KustoWriter<IN> implements SinkWriter<IN> {
  private final KustoConnectionOptions connectionOptions;
  private final KustoWriteOptions writeOptions;
  private final KustoSerializationSchema<IN> serializationSchema;
  private final KustoSinkContext sinkContext;
  private final MailboxExecutor mailboxExecutor;
  private final boolean flushOnCheckpoint;
  private final List<String> bulkRequests = new ArrayList<>();
  private final Collector<String> collector;
  private final Counter numRecordsOut;
  private final transient IngestClient ingestClient;

  private boolean checkpointInProgress = false;
  private volatile long lastSendTime = 0L;
  private volatile long ackTime = Long.MAX_VALUE;

  public KustoWriter(KustoConnectionOptions connectionOptions, KustoWriteOptions writeOptions,
      boolean flushOnCheckpoint, Sink.InitContext initContext,
      KustoSerializationSchema<IN> serializationSchema) throws URISyntaxException {
    this.connectionOptions = checkNotNull(connectionOptions);
    this.writeOptions = checkNotNull(writeOptions);
    this.serializationSchema = checkNotNull(serializationSchema);
    this.flushOnCheckpoint = flushOnCheckpoint;

    checkNotNull(initContext);
    this.mailboxExecutor = checkNotNull(initContext.getMailboxExecutor());

    SinkWriterMetricGroup metricGroup = checkNotNull(initContext.metricGroup());
    metricGroup.setCurrentSendTimeGauge(() -> ackTime - lastSendTime);

    this.numRecordsOut = metricGroup.getNumRecordsSendCounter();
    this.collector = new ListCollector<>(this.bulkRequests);

    // Initialize the serialization schema.
    this.sinkContext = new DefaultKustoSinkContext(initContext, writeOptions);
    try {
      SerializationSchema.InitializationContext initializationContext =
          initContext.asSerializationSchemaInitializationContext();
      serializationSchema.open(initializationContext, sinkContext, writeOptions);
    } catch (Exception e) {
      throw new FlinkRuntimeException("Failed to open the KustoEmitter", e);
    }

    // Initialize the Kusto client.
    ConnectionStringBuilder kustoConnectionStringBuilder =
        this.connectionOptions.isManagedIdentity()
            ? ConnectionStringBuilder.createWithAadManagedIdentity(
                this.connectionOptions.getClusterUrl(),
                this.connectionOptions.getManagedIdentityAppId())
            : ConnectionStringBuilder.createWithAadApplicationCredentials(
                this.connectionOptions.getClusterUrl(), this.connectionOptions.getAppId(),
                this.connectionOptions.getAppKey(), this.connectionOptions.getTenantId());

    this.ingestClient = IngestClientFactory.createClient(kustoConnectionStringBuilder);
  }

  @Override
  public void write(IN element, Context context) throws IOException, InterruptedException {
    while (checkpointInProgress) {
      mailboxExecutor.yield();
    }
    KustoRow kustoRow = this.serializationSchema.serialize(element, sinkContext);
    numRecordsOut.inc();
    collector.collect(kustoRow.toString());
    // if (isOverMaxBatchSizeLimit() || isOverMaxBatchIntervalLimit()) {
    // doBulkWrite();
    // }
  }

  @Override
  public void flush(boolean b) throws IOException, InterruptedException {

  }

  @Override
  public void close() throws Exception {

  }

  @VisibleForTesting
  void writeToBlob() throws IOException {
    if (bulkRequests.isEmpty()) {
      // no records to write
      return;
    }

  }
}
