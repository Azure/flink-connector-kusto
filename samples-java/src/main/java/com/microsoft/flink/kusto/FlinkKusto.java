package com.microsoft.flink.kusto;

import java.io.FileNotFoundException;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.opensky.model.StateVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.kusto.KustoWriteSink;

public class FlinkKusto {
  protected static final Logger LOG = LoggerFactory.getLogger(FlinkKusto.class);

  public static void main(String... args) {
    try {
      // load properties from file
      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      DataStream<StateVector> openSkyStream = env.addSource(new OpenSkyApiSource());
      String appId = "";
      String appKey = "";
      String cluster = "";
      String tenantId = "";
      String database = "";
      KustoConnectionOptions kustoConnectionOptions = KustoConnectionOptions.builder()
          .setAppId(appId).setAppKey(appKey).setTenantId(tenantId).setClusterUrl(cluster).build();
      String defaultTable = "StateVector";
      KustoWriteOptions kustoWriteOptions =
          KustoWriteOptions.builder().withDatabase(database).withTable(defaultTable)
              .withFlushImmediately(true).withBatchIntervalMs(1000).withBatchSize(10).build();
      KustoWriteSink.addSink(openSkyStream).setConnectionOptions(kustoConnectionOptions)
          .setWriteOptions(kustoWriteOptions).build().setParallelism(2)
          .name(defaultTable + " Kusto Sink")
          .uid(defaultTable + " Kusto Sink");
      env.execute("Flink Open Sky Demo");
    } catch (FileNotFoundException e) {
      LOG.error("FileNotFoundException", e);
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Failed with exception", e);
    }
  }
}
