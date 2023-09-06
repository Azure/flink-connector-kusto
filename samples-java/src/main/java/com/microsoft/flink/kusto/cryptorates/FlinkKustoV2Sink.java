package com.microsoft.flink.kusto.cryptorates;

import java.io.FileNotFoundException;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.kusto.KustoWriteSink;

public class FlinkKustoV2Sink {
  protected static final Logger LOG = LoggerFactory.getLogger(FlinkKustoV2Sink.class);

  public static void main(String... args) {
    try {
      final OutputTag<Heartbeat> outputTagHeartbeat =
          new OutputTag<Heartbeat>("side-output-heartbeat") {};
      final OutputTag<Ticker> outputTagTicker = new OutputTag<Ticker>("side-output-ticker") {};
      // load properties from file
      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      SplitTypes processSplitFunction = new SplitTypes();
      DataStream<String> cryptoSocketSource = env.addSource(new BTCWebSocketSource());
      DataStream<Heartbeat> heartbeatDataStream =
          cryptoSocketSource.process(processSplitFunction).getSideOutput(outputTagHeartbeat);
      DataStream<Ticker> tickerDataStream =
          cryptoSocketSource.process(processSplitFunction).getSideOutput(outputTagTicker);
      String appId = System.getenv("APP_ID");
      String appKey = System.getenv("APP_KEY");
      String cluster = System.getenv("CLUSTER_URL");
      String tenantId = System.getenv("AZURE_TENANT_ID");
      String database = System.getenv("E2E_DB");
      KustoConnectionOptions kustoConnectionOptions = KustoConnectionOptions.builder()
          .setAppId(appId).setAppKey(appKey).setTenantId(tenantId).setClusterUrl(cluster).build();
      String defaultTable = "CryptoRatesHeartbeatWithAck";
      KustoWriteOptions kustoWriteOptionsHeartbeat =
          KustoWriteOptions.builder().withDatabase(database).withTable(defaultTable)
              .withBatchSize(200).withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
      KustoWriteSink.builder().setWriteOptions(kustoWriteOptionsHeartbeat)
          .setConnectionOptions(kustoConnectionOptions).build(heartbeatDataStream);
      KustoWriteOptions kustoWriteOptionsTicker = KustoWriteOptions.builder().withDatabase(database)
          .withBatchSize(200).withTable("CryptoRatesTickerWithAck")
          .withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
      KustoWriteSink.builder().setWriteOptions(kustoWriteOptionsTicker)
          .setConnectionOptions(kustoConnectionOptions).build(tickerDataStream);
      env.executeAsync("Flink Crypto Rates Demo");
    } catch (FileNotFoundException e) {
      LOG.error("FileNotFoundException", e);
    } catch (Exception e) {
      LOG.error("Failed with exception", e);
    }
  }
}
