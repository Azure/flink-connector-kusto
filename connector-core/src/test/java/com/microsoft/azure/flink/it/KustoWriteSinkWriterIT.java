package com.microsoft.azure.flink.it;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple8;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.TestSinkInitContext;
import com.microsoft.azure.flink.TupleTestObject;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.writer.internal.sink.KustoSinkWriter;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import static com.microsoft.azure.flink.it.ITSetup.getConnectorProperties;
import static com.microsoft.azure.flink.it.ITSetup.getWriteOptions;
import static org.junit.jupiter.api.Assertions.fail;

@Execution(ExecutionMode.CONCURRENT)
public class KustoWriteSinkWriterIT {
  private static final Logger LOG = LoggerFactory.getLogger(KustoWriteSinkWriterIT.class);
  private static Client engineClient;
  private static Client dmClient;
  private static KustoConnectionOptions coordinates;
  private static KustoWriteOptions writeOptions;
  private static TestSinkInitContext sinkInitContext;
  private final TypeInformation<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> tuple8TypeInformation =
      TypeInformation.of(
          new TypeHint<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>>() {});

  private final TypeSerializer<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> tuple8TypeSerializer =
      tuple8TypeInformation.createSerializer(new ExecutionConfig());

  @BeforeAll
  public static void setUp() {
    ConnectionStringBuilder engineCsb = null;
    ConnectionStringBuilder dmCsb = null;
    coordinates = getConnectorProperties();
    writeOptions = getWriteOptions();
    coordinates = getConnectorProperties();
    sinkInitContext = new TestSinkInitContext();
    if (StringUtils.isNotEmpty(coordinates.getAppId())
        && StringUtils.isNotEmpty(coordinates.getAppKey())
        && StringUtils.isNotEmpty(coordinates.getTenantId())
        && StringUtils.isNotEmpty(coordinates.getClusterUrl())) {
      LOG.error("Connecting to cluster: {}", coordinates.getClusterUrl());
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
      LOG.info("Creating tables in Kusto");
      KustoTestUtil.createTables(engineClient, writeOptions);
      KustoTestUtil.refreshDm(dmClient, writeOptions);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @AfterAll
  public static void tearDown() throws Exception {
    engineClient.execute(writeOptions.getDatabase(),
        String.format(".drop table %s", writeOptions.getTable()));
    LOG.error("Finished table clean up. Dropped table {}", writeOptions.getTable());
    dmClient.close();
    engineClient.close();
  }

  @Test
  public void testSinkTupleIngest() throws Exception {
    String typeKey = "FlinkTupleTest-SinkWriter";
    try (
        KustoSinkWriter<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> kustoSinkWriter =
            new KustoSinkWriter<>(coordinates, writeOptions, tuple8TypeSerializer,
                tuple8TypeInformation, true, sinkInitContext)) {
      int maxRecords = 100; // A random number for testing both time and record count based flush
      Map<String, String> expectedResults = new HashMap<>();
      for (int x = 0; x < maxRecords; x++) {
        TupleTestObject tupleTestObject = new TupleTestObject(x, typeKey);
        kustoSinkWriter.write(tupleTestObject.toTuple(), null);
        expectedResults.put(tupleTestObject.getVstr(), tupleTestObject.toJsonString());
      }
      LOG.info("Finished writing records to sink, performing assertions");
      performTest(kustoSinkWriter, expectedResults, maxRecords, typeKey);
    }
  }

  private void performTest(KustoSinkWriter<?> kustoSinkWriter, Map<String, String> expectedResults,
      int maxRecords, String typeKey) {
    try {
      // Perform the assertions here
      LOG.info(
          "Calling flush on checkpoint or end of input so that the writer to flush all pending data for at-least-once");
      kustoSinkWriter.flush(true);
      KustoTestUtil.performAssertions(engineClient, writeOptions, expectedResults, maxRecords,
          typeKey);
    } catch (Exception e) {
      LOG.error("Failed to create KustoGenericWriteAheadSink", e);
      fail(e);
    }
  }
}
