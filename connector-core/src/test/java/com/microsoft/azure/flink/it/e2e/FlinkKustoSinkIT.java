package com.microsoft.azure.flink.it.e2e;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.TupleTestObject;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.it.KustoTestUtil;
import com.microsoft.azure.kusto.KustoWriteSinkV2;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import static com.microsoft.azure.flink.it.ITSetup.getConnectorProperties;
import static com.microsoft.azure.flink.it.ITSetup.getWriteOptions;

public class FlinkKustoSinkIT {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkKustoSinkIT.class);
  private static Client engineClient;
  private static Client dmClient;
  private static KustoConnectionOptions coordinates;

  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension(
      new MiniClusterResourceConfiguration.Builder().setNumberTaskManagers(2).build());
  @RegisterExtension
  final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

  @BeforeAll
  public static void setUp() {
    coordinates = getConnectorProperties();
    coordinates = getConnectorProperties();
    if (StringUtils.isNotEmpty(coordinates.getAppId())
        && StringUtils.isNotEmpty(coordinates.getAppKey())
        && StringUtils.isNotEmpty(coordinates.getTenantId())
        && StringUtils.isNotEmpty(coordinates.getClusterUrl())) {
      LOG.info("KustoSinkE2ETests : Connecting to cluster: {}", coordinates.getClusterUrl());
      ConnectionStringBuilder engineCsb =
          ConnectionStringBuilder.createWithAadApplicationCredentials(coordinates.getClusterUrl(),
              coordinates.getAppId(), coordinates.getAppKey(), coordinates.getTenantId());
      ConnectionStringBuilder dmCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
          coordinates.getClusterUrl().replaceAll("https://", "https://ingest-"),
          coordinates.getAppId(), coordinates.getAppKey(), coordinates.getTenantId());
      try {
        engineClient = ClientFactory.createClient(engineCsb);
        dmClient = ClientFactory.createClient(dmCsb);
        LOG.info("Creating tables for KustoSinkE2ETests");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      LOG.info("Skipping test due to missing configuration in KustoSinkE2ETests");
    }
  }

  @AfterAll
  public static void tearDown() throws Exception {
    dmClient.close();
    engineClient.close();
  }

  @ParameterizedTest
  @EnumSource(value = DeliveryGuarantee.class, mode = EnumSource.Mode.EXCLUDE,
      names = "EXACTLY_ONCE")
  void testWriteToKustoWithExactlyOnce(DeliveryGuarantee deliveryGuarantee) throws Exception {
    final String typeKey = "sink-with-delivery-" + deliveryGuarantee;
    KustoWriteOptions kustoWriteOptions = getWriteOptions(100, 30, deliveryGuarantee);
    // create the tables
    KustoTestUtil.createTables(engineClient, kustoWriteOptions);
    KustoTestUtil.refreshDm(dmClient, kustoWriteOptions);
    int maxRecords = 221;
    // Run the flink job
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(100L);
    env.setRestartStrategy(RestartStrategies.noRestart());
    // send these records through the stream
    DataStream<TupleTestObject> stream =
        env.fromSequence(1, maxRecords).map(new RichMapFunction<Long, TupleTestObject>() {
          @Override
          public TupleTestObject map(Long x) {
            return new TupleTestObject(x.intValue(), typeKey);
          }
        });
    KustoWriteSinkV2.builder().setWriteOptions(kustoWriteOptions).setConnectionOptions(coordinates)
        .build(stream, 2);
    env.execute();
    // expected results
    Map<String, String> expectedResults = new HashMap<>();
    for (int i = 0; i < maxRecords; i++) {
      expectedResults.put(String.valueOf(i), new TupleTestObject(i, typeKey).toJsonString());
    }
    KustoTestUtil.performAssertions(engineClient, kustoWriteOptions, expectedResults, maxRecords,
        typeKey);
    // Clean up the tables
    engineClient.execute(kustoWriteOptions.getDatabase(),
        String.format(".drop table %s", kustoWriteOptions.getTable()));
    LOG.info("Finished table clean up. Dropped table {}", kustoWriteOptions.getTable());
  }
}
