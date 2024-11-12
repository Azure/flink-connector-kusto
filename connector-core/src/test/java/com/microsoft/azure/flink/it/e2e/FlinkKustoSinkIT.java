package com.microsoft.azure.flink.it.e2e;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.TupleTestObject;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.it.KustoTestUtil;
import com.microsoft.azure.kusto.KustoWriteSink;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import static com.microsoft.azure.flink.it.ITSetup.getConnectorProperties;
import static com.microsoft.azure.flink.it.ITSetup.getWriteOptions;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlinkKustoSinkIT {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkKustoSinkIT.class);
  private static Client engineClient;
  private static Client dmClient;
  private static KustoConnectionOptions coordinates;
  private static final int MAX_RECORDS = 249;

  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension(
      new MiniClusterResourceConfiguration.Builder().setNumberTaskManagers(2)
          .setShutdownTimeout(Time.fromDuration(Duration.ofMinutes(5))).build());
  @RegisterExtension
  final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();
  static final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();

  @BeforeAll
  public static void setUp() {
    env.enableCheckpointing(100L);
    env.setRestartStrategy(RestartStrategies.noRestart());
    coordinates = getConnectorProperties();
    coordinates = getConnectorProperties();
    ConnectionStringBuilder engineCsb = null;
    ConnectionStringBuilder dmCsb = null;
    if (StringUtils.isNotEmpty(coordinates.getAppId())
        && StringUtils.isNotEmpty(coordinates.getAppKey())
        && StringUtils.isNotEmpty(coordinates.getTenantId())
        && StringUtils.isNotEmpty(coordinates.getClusterUrl())) {
      LOG.info("KustoSinkE2ETests : Connecting to cluster: {}", coordinates.getClusterUrl());
      engineCsb =
          ConnectionStringBuilder.createWithAadApplicationCredentials(coordinates.getClusterUrl(),
              coordinates.getAppId(), coordinates.getAppKey(), coordinates.getTenantId());
      dmCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
          coordinates.getClusterUrl().replaceAll("https://", "https://ingest-"),
          coordinates.getAppId(), coordinates.getAppKey(), coordinates.getTenantId());
    } else {
      engineCsb = ConnectionStringBuilder.createWithAzureCli(coordinates.getClusterUrl());
      dmCsb = ConnectionStringBuilder.createWithAzureCli(
          coordinates.getClusterUrl().replaceAll("https://", "https://ingest-"));
    }
    try {
      engineClient = ClientFactory.createClient(engineCsb);
      dmClient = ClientFactory.createClient(dmCsb);
      LOG.info("Creating tables for KustoSinkE2ETests");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterAll
  public static void tearDown() throws Exception {
    dmClient.close();
    engineClient.close();
    env.close();
  }

  @ParameterizedTest
  @EnumSource(value = DeliveryGuarantee.class)
  void testWriteToKustoWithDeliverySemantics(DeliveryGuarantee deliveryGuarantee) throws Exception {
    final String typeKey = "sink-with-delivery-" + deliveryGuarantee;
    KustoWriteOptions kustoWriteOptions = getWriteOptions(1000, 100, deliveryGuarantee);
    // create the tables
    KustoTestUtil.createTables(engineClient, kustoWriteOptions);
    KustoTestUtil.refreshDm(dmClient, kustoWriteOptions);
    // Generate a few records to send
    Map<String, TupleTestObject> dataToSend = new HashMap<>();
    for (int i = 0; i < MAX_RECORDS; i++) {
      TupleTestObject record = new TupleTestObject(i, typeKey);
      dataToSend.put(record.getVstr(), record);
    }
    // send these records through the stream
    DataStream<TupleTestObject> stream = env.fromCollection(dataToSend.values());
    KustoWriteSink.builder().setWriteOptions(kustoWriteOptions).setConnectionOptions(coordinates)
        .build(stream, 2);
    env.execute();
    // expected results
    Map<String, String> expectedResults = dataToSend.keySet().parallelStream()
        .collect(Collectors.toMap(Function.identity(), k -> dataToSend.get(k).toJsonString()));
    KustoTestUtil.performAssertions(engineClient, kustoWriteOptions, expectedResults, MAX_RECORDS,
        typeKey);
    // Clean up the tables
    engineClient.execute(kustoWriteOptions.getDatabase(),
        String.format(".drop table %s", kustoWriteOptions.getTable()));
    LOG.info("Finished table clean up. Dropped table {}", kustoWriteOptions.getTable());
  }

  @ParameterizedTest
  @EnumSource(value = DeliveryGuarantee.class)
  void testWriteToKustoWithDeliverySemanticsGenericWriteAheadSink(
      DeliveryGuarantee deliveryGuarantee) throws Exception {
    final String typeKey = "sink-with-delivery-" + deliveryGuarantee;
    KustoWriteOptions kustoWriteOptions = getWriteOptions(1000, 100, deliveryGuarantee);
    // create the tables
    KustoTestUtil.createTables(engineClient, kustoWriteOptions);
    KustoTestUtil.refreshDm(dmClient, kustoWriteOptions);
    Map<String, TupleTestObject> dataToSend = new HashMap<>();
    for (int i = 0; i < MAX_RECORDS; i++) {
      TupleTestObject record = new TupleTestObject(i, typeKey);
      dataToSend.put(record.getVstr(), record);
    }
    // send these records through the stream
    DataStream<TupleTestObject> stream = env.fromCollection(dataToSend.values());
    KustoWriteSink.builder().setWriteOptions(kustoWriteOptions).setConnectionOptions(coordinates)
        .buildWriteAheadSink(stream, 2);
    env.execute();
    // expected results
    Map<String, String> expectedResults = dataToSend.keySet().parallelStream()
        .collect(Collectors.toMap(Function.identity(), k -> dataToSend.get(k).toJsonString()));
    KustoTestUtil.performAssertions(engineClient, kustoWriteOptions, expectedResults, MAX_RECORDS,
        typeKey);
    // Clean up the tables
    engineClient.execute(kustoWriteOptions.getDatabase(),
        String.format(".drop table %s", kustoWriteOptions.getTable()));
    LOG.info("Finished table clean up. Dropped table {}", kustoWriteOptions.getTable());
  }

  @Test
  void testRecovery() throws Exception {
    // perform the test where there is intermittent failure, and the job recovers and keeps sending
    // the data
    // this shoud get ingested (though ultimately the JobStatus goes to failure status)
    final String typeKey = "atleast-once-test-recovery";
    KustoWriteOptions kustoWriteOptions =
        getWriteOptions(1000, 100, DeliveryGuarantee.AT_LEAST_ONCE);
    Map<String, TupleTestObject> dataToSend = new HashMap<>();
    for (int i = 0; i < MAX_RECORDS; i++) {
      TupleTestObject record = new TupleTestObject(i, typeKey);
      dataToSend.put(record.getVstr(), record);
    }
    Map<String, String> expectedResults = dataToSend.keySet().parallelStream()
        .collect(Collectors.toMap(Function.identity(), k -> dataToSend.get(k).toJsonString()));
    final SharedReference<AtomicBoolean> failed = sharedObjects.add(new AtomicBoolean(false));
    StreamExecutionEnvironment envFail = StreamExecutionEnvironment.getExecutionEnvironment();
    envFail.enableCheckpointing(100L);
    envFail.setRestartStrategy(RestartStrategies.noRestart());
    // create the tables
    KustoTestUtil.createTables(engineClient, kustoWriteOptions);
    KustoTestUtil.refreshDm(dmClient, kustoWriteOptions);
    // send these records through the stream
    DataStream<TupleTestObject> stream =
        envFail.fromCollection(dataToSend.values()).map(new FailingMapper(failed))
            .uid("failing-mapper").name("failing-mapper").disableChaining();
    KustoWriteSink.builder().setWriteOptions(kustoWriteOptions).setConnectionOptions(coordinates)
        .build(stream, 2);
    envFail.executeAsync().getJobExecutionResult().whenComplete((aVoid, ex) -> {
      if (ex != null) {
        // this block is expected to be hit as the failing mapper will cause the job to fail
        LOG.debug("Failed with exception", ex);
        assertTrue(failed.get().get());
        KustoTestUtil.performAssertions(engineClient, kustoWriteOptions, expectedResults,
            MAX_RECORDS, typeKey);
        // Clean up the tables
        try {
          engineClient.execute(kustoWriteOptions.getDatabase(),
              String.format(".drop table %s", kustoWriteOptions.getTable()));
          LOG.info("Finished table clean up (recovery tests). Dropped table {}",
              kustoWriteOptions.getTable());
        } catch (DataServiceException | DataClientException ignored) {
        }
      }
    });
  }

  private static class FailingMapper
      implements MapFunction<TupleTestObject, TupleTestObject>, CheckpointListener {
    private final SharedReference<AtomicBoolean> failed;
    private int emittedRecords = 0;

    private FailingMapper(SharedReference<AtomicBoolean> failed) {
      this.failed = failed;
    }

    @Override
    public TupleTestObject map(TupleTestObject value) throws Exception {
      Thread.sleep(50);
      emittedRecords++;
      return value;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
      if (failed.get().get() || emittedRecords == 0) {
        return;
      }
      failed.get().set(true);
      throw new Exception("Expected failure");
    }
  }
}
