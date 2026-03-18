package com.microsoft.azure.flink.quickstart;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.kusto.KustoWriteSink;

/**
 * Flink job that reads JSON sensor readings from Kafka and writes them to Azure Data Explorer
 * (Kusto) using the flink-connector-kusto.
 *
 * <p>
 * Usage:
 *
 * <pre>
 * flink run job.jar \
 *   --kafka.bootstrap.servers kafka:9092 \
 *   --kafka.topic sensor-readings \
 *   --kusto.cluster.url https://cluster.region.kusto.windows.net \
 *   --kusto.database mydb \
 *   --kusto.table FlinkQuickstart \
 *   --kusto.app.id &lt;app-id&gt; \
 *   --kusto.app.key &lt;app-key&gt; \
 *   --kusto.tenant.id &lt;tenant-id&gt;
 * </pre>
 */
public class KafkaToKustoJob {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToKustoJob.class);

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);

    String bootstrapServers = params.getRequired("kafka.bootstrap.servers");
    String topic = params.getRequired("kafka.topic");
    String clusterUrl = params.getRequired("kusto.cluster.url");
    String database = params.getRequired("kusto.database");
    String table = params.getRequired("kusto.table");
    String appId = params.getRequired("kusto.app.id");
    String appKey = params.getRequired("kusto.app.key");
    String tenantId = params.getRequired("kusto.tenant.id");

    LOG.info("Starting Kafka to Kusto pipeline: topic={} -> {}.{}", topic, database, table);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(60_000);

    // Kafka Source - reads JSON strings
    KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
        .setBootstrapServers(bootstrapServers).setTopics(topic)
        .setGroupId("flink-kusto-quickstart")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema()).build();

    DataStream<String> kafkaStream =
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

    // Parse JSON to POJO
    DataStream<SensorReading> sensorStream =
        kafkaStream.map(new JsonDeserializer()).returns(SensorReading.class);

    // Kusto Sink
    KustoConnectionOptions connectionOptions = KustoConnectionOptions.builder().withAppId(appId)
        .withAppKey(appKey).withTenantId(tenantId).withClusterUrl(clusterUrl).build();

    KustoWriteOptions writeOptions = KustoWriteOptions.builder().withDatabase(database)
        .withTable(table).withBatchIntervalMs(30_000)
        .withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

    KustoWriteSink.builder().setConnectionOptions(connectionOptions).setWriteOptions(writeOptions)
        .build(sensorStream, 2);

    env.execute("Kafka to Kusto Quickstart");
  }

  /** Deserializes JSON strings into SensorReading POJOs. */
  public static class JsonDeserializer extends RichMapFunction<String, SensorReading> {
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
      mapper = new ObjectMapper();
    }

    @Override
    public SensorReading map(String json) throws Exception {
      return mapper.readValue(json, SensorReading.class);
    }
  }
}
