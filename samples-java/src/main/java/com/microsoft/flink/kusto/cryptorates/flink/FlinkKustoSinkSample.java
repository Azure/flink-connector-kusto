package com.microsoft.flink.kusto.cryptorates.flink;

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
import com.microsoft.flink.kusto.cryptorates.common.Heartbeat;
import com.microsoft.flink.kusto.cryptorates.common.Ticker;

public class FlinkKustoSinkSample {
  protected static final Logger LOG = LoggerFactory.getLogger(FlinkKustoSinkSample.class);

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
      // Ref this for Azure Identity examples.
      // https://blog.jongallant.com/2021/08/azure-identity-202/
      String appId = System.getenv("AZURE_CLIENT_ID");
      String appKey = System.getenv("AZURE_CLIENT_SECRET");
      String tenantId = System.getenv("AZURE_TENANT_ID");
      String database = System.getenv("FLINK_DB");
      String cluster = System.getenv("FLINK_CLUSTER_URI");
      KustoConnectionOptions kustoConnectionOptions =
          KustoConnectionOptions.builder().withAppId(appId).withAppKey(appKey)
              .withTenantId(tenantId).withClusterUrl(cluster).build();
      String defaultTable = "CryptoRatesHeartbeatTimeBatch";
      KustoWriteOptions kustoWriteOptionsHeartbeat = KustoWriteOptions.builder()
          .withDatabase(database).withTable(defaultTable).withBatchIntervalMs(30000)
          .withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
      KustoWriteSink.builder().setWriteOptions(kustoWriteOptionsHeartbeat)
          .setConnectionOptions(kustoConnectionOptions).build(heartbeatDataStream, 4);
      KustoWriteOptions kustoWriteOptionsTicker = KustoWriteOptions.builder().withDatabase(database)
          .withTable("CryptoRatesTickerTimeBatch").withBatchIntervalMs(30000)
          .withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
      KustoWriteSink.builder().setWriteOptions(kustoWriteOptionsTicker)
          .setConnectionOptions(kustoConnectionOptions).build(tickerDataStream, 4);
      env.executeAsync("Flink Crypto Rates Demo - Batch of 30s");
    } catch (FileNotFoundException e) {
      LOG.error("FileNotFoundException", e);
    } catch (Exception e) {
      LOG.error("Failed with exception", e);
    }
  }
}
