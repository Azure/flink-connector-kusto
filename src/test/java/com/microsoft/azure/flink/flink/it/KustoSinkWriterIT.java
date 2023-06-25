package com.microsoft.azure.flink.flink.it;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple8;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.flink.TestSinkInitContext;
import com.microsoft.azure.flink.flink.TupleTestObject;
import com.microsoft.azure.flink.writer.internal.sink.KustoSinkWriter;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import static com.microsoft.azure.flink.flink.ITSetup.getConnectorProperties;
import static com.microsoft.azure.flink.flink.ITSetup.getWriteOptions;
import static org.junit.jupiter.api.Assertions.fail;

public class KustoSinkWriterIT {
  private static final Logger LOG = LoggerFactory.getLogger(KustoSinkWriterIT.class);
  private static final String KEY_COL = "vstr";
  private static Client engineClient;
  private static Client dmClient;
  private static KustoConnectionOptions coordinates;
  private static KustoWriteOptions writeOptions;
  private static TestSinkInitContext sinkInitContext;

  private final TypeInformation<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> tuple8TypeInformation =
      TypeInformation.of(new TypeHint<>() {});
  private final TypeSerializer<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> tuple8TypeSerializer =
      tuple8TypeInformation.createSerializer(new ExecutionConfig());


  @BeforeAll
  public static void setUp() {
    coordinates = getConnectorProperties();
    writeOptions = getWriteOptions();
    coordinates = getConnectorProperties();
    sinkInitContext = new TestSinkInitContext();
    if (StringUtils.isNotEmpty(coordinates.getAppId())
        && StringUtils.isNotEmpty(coordinates.getAppKey())
        && StringUtils.isNotEmpty(coordinates.getTenantId())
        && StringUtils.isNotEmpty(coordinates.getClusterUrl())) {
      LOG.error("Connecting to cluster: {}", coordinates.getClusterUrl());
      ConnectionStringBuilder engineCsb =
          ConnectionStringBuilder.createWithAadApplicationCredentials(coordinates.getClusterUrl(),
              coordinates.getAppId(), coordinates.getAppKey(), coordinates.getTenantId());
      ConnectionStringBuilder dmCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
          coordinates.getClusterUrl().replaceAll("https://", "https://ingest-"),
          coordinates.getAppId(), coordinates.getAppKey(), coordinates.getTenantId());
      try {
        engineClient = ClientFactory.createClient(engineCsb);
        dmClient = ClientFactory.createClient(dmCsb);
        LOG.info("Creating tables in Kusto");
        createTables();
        refreshDm();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      LOG.info("Skipping test due to missing configuration");
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


  // https://github.com/apache/flink/blob/master/flink-streaming-java/src/test/java/org/apache/flink/streaming/runtime/operators/GenericWriteAheadSinkTest.java
  private static void createTables() throws Exception {
    URL kqlResource = KustoSinkWriterIT.class.getClassLoader().getResource("it-setup.kql");
    assert kqlResource != null;
    List<String> kqlsToExecute = Files.readAllLines(Paths.get(kqlResource.toURI())).stream()
        .map(kql -> kql.replace("TBL", writeOptions.getTable())).collect(Collectors.toList());
    kqlsToExecute.forEach(kql -> {
      try {
        engineClient.execute(writeOptions.getDatabase(), kql);
      } catch (Exception e) {
        LOG.error("Failed to execute kql: {}", kql, e);
      }
    });
    LOG.info("Created table {} and associated mappings", writeOptions.getTable());
  }

  private static void refreshDm() throws Exception {
    URL kqlResource = KustoSinkWriterIT.class.getClassLoader().getResource("policy-refresh.kql");
    assert kqlResource != null;
    List<String> kqlsToExecute = Files.readAllLines(Paths.get(kqlResource.toURI())).stream()
        .map(kql -> kql.replace("TBL", writeOptions.getTable()))
        .map(kql -> kql.replace("DB", writeOptions.getDatabase())).collect(Collectors.toList());
    kqlsToExecute.forEach(kql -> {
      try {
        dmClient.execute(kql);
      } catch (Exception e) {
        LOG.error("Failed to execute DM kql: {}", kql, e);
      }
    });
    LOG.info("Refreshed cache on DB {}", writeOptions.getDatabase());
  }


  // @Test
  // void testWriteOnBulkFlush() throws Exception {
  // final String typeKey = "test-bulk-flush-without-checkpoint";
  // final boolean flushOnCheckpoint = false;
  // final int batchSize = 5;
  // final int batchIntervalMs = -1;
  // try (KustoSinkWriter<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>>
  // kustoSinkWriter =
  // new KustoSinkWriter<>(coordinates, getWriteOptions(batchIntervalMs,batchSize),
  // tuple8TypeSerializer, tuple8TypeInformation, true,
  // sinkInitContext)) {
  // kustoSinkWriter.write(new TupleTestObject(201, typeKey).toTuple(), null);
  // kustoSinkWriter.write(new TupleTestObject(202, typeKey).toTuple(), null);
  // kustoSinkWriter.write(new TupleTestObject(203, typeKey).toTuple(), null);
  // kustoSinkWriter.write(new TupleTestObject(204, typeKey).toTuple(), null);
  // // Ignore flush on checkpoint
  // kustoSinkWriter.flush(flushOnCheckpoint);
  // assertThatIdsAreNotWritten(collectionOf(collection), 1, 2, 3, 4);
  // // Trigger flush
  // kustoSinkWriter.write(buildMessage(5), null);
  // assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3, 4, 5);
  // kustoSinkWriter.write(buildMessage(6), null);
  // assertThatIdsAreNotWritten(collectionOf(collection), 6);
  // // Force flush
  // kustoSinkWriter.doBulkWrite();
  // assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3, 4, 5, 6);
  // }
  // }



  //
  // @Test
  // void testWriteOnBatchIntervalFlush() throws Exception {
  // final String collection = "test-bulk-flush-with-interval";
  // final boolean flushOnCheckpoint = false;
  // final int batchSize = -1;
  // final int batchIntervalMs = 1000;
  //
  // try (final MongoWriter<Document> writer =
  // createWriter(collection, batchSize, batchIntervalMs, flushOnCheckpoint)) {
  // writer.write(buildMessage(1), null);
  // writer.write(buildMessage(2), null);
  // writer.write(buildMessage(3), null);
  // writer.write(buildMessage(4), null);
  // writer.doBulkWrite();
  // }
  //
  // assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3, 4);
  // }
}
