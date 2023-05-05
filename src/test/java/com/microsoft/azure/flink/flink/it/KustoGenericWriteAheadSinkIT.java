package com.microsoft.azure.flink.flink.it;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.api.scala.typeutils.ScalaCaseClassSerializer;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.json.JSONException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.flink.TupleTestObject;
import com.microsoft.azure.flink.writer.internal.KustoGenericWriteAheadSink;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

import static com.microsoft.azure.flink.flink.ITSetup.getConnectorProperties;
import static com.microsoft.azure.flink.flink.ITSetup.getWriteOptions;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

@SuppressWarnings({"rawtypes", "unchecked"})
public class KustoGenericWriteAheadSinkIT {
  private static final Logger LOG = LoggerFactory.getLogger(KustoGenericWriteAheadSinkIT.class);

  private static final String KEY_COL = "vstr";
  private static Client engineClient;
  private static Client dmClient;
  private static KustoConnectionOptions coordinates;
  private static KustoWriteOptions writeOptions;

  @BeforeAll
  public static void setUp() {
    coordinates = getConnectorProperties();
    writeOptions = getWriteOptions();
    coordinates = getConnectorProperties();
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
  public void testTupleIngest() throws Exception {
    String typeKey = "FlinkTupleTest";
    TypeSerializer<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> serializer =
        TypeInformation.of(
            new TypeHint<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>>() {})
            .createSerializer(new ExecutionConfig());
    KustoGenericWriteAheadSink<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> kustoGenericWriteAheadSink =
        new KustoGenericWriteAheadSink<>(coordinates, writeOptions, new SimpleCommitter(),
            serializer, UUID.randomUUID().toString());
    OneInputStreamOperatorTestHarness<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>, ?> testHarness =
        new OneInputStreamOperatorTestHarness<>(kustoGenericWriteAheadSink);
    int maxRecords = 100;
    Map<String, String> expectedResults = new HashMap<>();
    testHarness.open();
    for (int x = 0; x < maxRecords; x++) {
      TupleTestObject tupleTestObject = new TupleTestObject(x, typeKey);
      testHarness.processElement(new StreamRecord(tupleTestObject.toTuple()));
      expectedResults.put(tupleTestObject.getVstr(), tupleTestObject.toJsonString());
    }
    performTest(testHarness, expectedResults, maxRecords, typeKey);
  }

  @Test
  public void testRowIngest() throws Exception {
    String typeKey = "FlinkRowTest";
    ExecutionConfig config = new ExecutionConfig();
    TypeSerializer<?>[] fieldSerializers = new TypeSerializer[8];
    TypeInformation<?>[] types = new TypeInformation[8];
    types[0] = TypeInformation.of(new TypeHint<Integer>() {});
    types[1] = TypeInformation.of(new TypeHint<Double>() {});
    types[2] = TypeInformation.of(new TypeHint<String>() {});
    types[3] = TypeInformation.of(new TypeHint<Boolean>() {});
    types[4] = TypeInformation.of(new TypeHint<Double>() {});
    types[5] = TypeInformation.of(new TypeHint<String>() {});
    types[6] = TypeInformation.of(new TypeHint<Long>() {});
    types[7] = TypeInformation.of(new TypeHint<String>() {});
    for (int i = 0; i < 8; i++) {
      fieldSerializers[i] = types[i].createSerializer(config);
    }
    RowSerializer rowSerializer = new RowSerializer(fieldSerializers);
    KustoGenericWriteAheadSink<Row> kustoGenericWriteAheadSink =
        new KustoGenericWriteAheadSink<>(coordinates, writeOptions, new SimpleCommitter(),
            rowSerializer, UUID.randomUUID().toString());
    OneInputStreamOperatorTestHarness<Row, ?> testHarness =
        new OneInputStreamOperatorTestHarness<>(kustoGenericWriteAheadSink);
    int maxRecords = 100;
    Map<String, String> expectedResults = new HashMap<>();
    testHarness.open();
    for (int x = 0; x < maxRecords; x++) {
      TupleTestObject tupleTestObject = new TupleTestObject(x, typeKey);
      Row row = new Row(8);
      row.setField(0, tupleTestObject.getVnum());
      row.setField(1, tupleTestObject.getVdec());
      row.setField(2, tupleTestObject.getVdate());
      row.setField(3, tupleTestObject.isVb());
      row.setField(4, tupleTestObject.getVreal());
      row.setField(5, tupleTestObject.getVstr());
      row.setField(6, tupleTestObject.getVlong());
      row.setField(7, tupleTestObject.getType());
      testHarness.processElement(new StreamRecord(row));
      expectedResults.put(tupleTestObject.getVstr(), tupleTestObject.toJsonString());
    }
    performTest(testHarness, expectedResults, maxRecords, typeKey);
  }

  @Test
  public void testCaseClassIngest() throws Exception {
    String typeKey = "FlinkCaseClassTest";
    ExecutionConfig config = new ExecutionConfig();
    TypeSerializer<?>[] fieldSerializers = new TypeSerializer[8];
    TypeInformation<?>[] types = new TypeInformation[8];
    types[0] = TypeInformation.of(new TypeHint<Integer>() {});
    types[1] = TypeInformation.of(new TypeHint<Double>() {});
    types[2] = TypeInformation.of(new TypeHint<String>() {});
    types[3] = TypeInformation.of(new TypeHint<Boolean>() {});
    types[4] = TypeInformation.of(new TypeHint<Double>() {});
    types[5] = TypeInformation.of(new TypeHint<String>() {});
    types[6] = TypeInformation.of(new TypeHint<Long>() {});
    types[7] = TypeInformation.of(new TypeHint<String>() {});
    for (int i = 0; i < 8; i++) {
      fieldSerializers[i] = types[i].createSerializer(config);
    }

    KustoGenericWriteAheadSink<Row> kustoGenericWriteAheadSink =
        new KustoGenericWriteAheadSink<>(coordinates, writeOptions, new SimpleCommitter(),
            new ScalaCaseClassSerializer(scala.Tuple8.class, fieldSerializers),
            UUID.randomUUID().toString());
    OneInputStreamOperatorTestHarness<Row, ?> testHarness =
        new OneInputStreamOperatorTestHarness<>(kustoGenericWriteAheadSink);
    int maxRecords = 100;
    Map<String, String> expectedResults = new HashMap<>();
    testHarness.open();
    for (int x = 0; x < maxRecords; x++) {
      TupleTestObject tupleTestObject = new TupleTestObject(x, typeKey);
      scala.Tuple8<Integer, Double, String, Boolean, Double, String, Long, String> scalaRow =
          new scala.Tuple8<>(tupleTestObject.getVnum(), tupleTestObject.getVdec(),
              tupleTestObject.getVdate(), tupleTestObject.isVb(), tupleTestObject.getVreal(),
              tupleTestObject.getVstr(), tupleTestObject.getVlong(), tupleTestObject.getType());
      testHarness.processElement(new StreamRecord(scalaRow));
      expectedResults.put(tupleTestObject.getVstr(), tupleTestObject.toJsonString());
    }
    performTest(testHarness, expectedResults, maxRecords, typeKey);
  }

  private void performTest(OneInputStreamOperatorTestHarness<?, ?> testHarness,
      Map<String, String> expectedResults, int maxRecords, String typeKey) {
    try {
      long checkpointId = Instant.now().toEpochMilli();
      testHarness.snapshot(checkpointId, Instant.now().toEpochMilli());
      testHarness.notifyOfCompletedCheckpoint(checkpointId);
      testHarness.close();
      // Perform the assertions here
      Map<String, String> actualRecordsIngested = getActualRecordsIngested(maxRecords, typeKey);
      actualRecordsIngested.keySet().parallelStream().forEach(key -> {
        LOG.debug("Record queried: {} and expected record {} ", actualRecordsIngested.get(key),
            expectedResults.get(key));
        try {
          JSONAssert.assertEquals(expectedResults.get(key), actualRecordsIngested.get(key),
              new CustomComparator(LENIENT,
                  // there are sometimes round off errors in the double values but they are close
                  // enough to 8 precision
                  new Customization("vdec",
                      (vdec1,
                          vdec2) -> Math.abs(Double.parseDouble(vdec1.toString())
                              - Double.parseDouble(vdec2.toString())) < 0.000000001),

                  new Customization("vdate", (vdate1, vdate2) -> Instant.parse(vdate1.toString())
                      .toEpochMilli() == Instant.parse(vdate2.toString()).toEpochMilli())));
        } catch (JSONException e) {
          fail(e);
        }
      });
      assertEquals(maxRecords, actualRecordsIngested.size());
    } catch (Exception e) {
      LOG.error("Failed to create KustoGenericWriteAheadSink", e);
      fail(e);
    }
  }

  // https://github.com/apache/flink/blob/master/flink-streaming-java/src/test/java/org/apache/flink/streaming/runtime/operators/GenericWriteAheadSinkTest.java
  private static void createTables() throws Exception {
    URL kqlResource =
        KustoGenericWriteAheadSinkIT.class.getClassLoader().getResource("it-setup.kql");
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
    URL kqlResource =
        KustoGenericWriteAheadSinkIT.class.getClassLoader().getResource("policy-refresh.kql");
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

  private Map<String, String> getActualRecordsIngested(int maxRecords, String typeKey) {
    String query = String.format(
        "%s | where type == '%s'| project  %s,vresult = pack_all() | order by vstr asc ",
        writeOptions.getTable(), typeKey, KEY_COL);
    Predicate<Object> predicate = (results) -> {
      if (results != null) {
        LOG.debug("Retrieved records count {}", ((Map<?, ?>) results).size());
      }
      return results == null || ((Map<?, ?>) results).isEmpty()
          || ((Map<?, ?>) results).size() < maxRecords;
    };
    // Waits 30 seconds for the records to be ingested. Repeats the poll 5 times , in all 150
    // seconds
    RetryConfig config = RetryConfig.custom().maxAttempts(5).retryOnResult(predicate)
        .waitDuration(Duration.of(30, SECONDS)).build();
    RetryRegistry registry = RetryRegistry.of(config);
    Retry retry = registry.retry("ingestRecordService", config);
    Supplier<Map<String, String>> recordSearchSupplier = () -> {
      try {
        LOG.debug("Executing query {} ", query);
        KustoResultSetTable resultSet =
            engineClient.execute(writeOptions.getDatabase(), query).getPrimaryResults();
        Map<String, String> actualResults = new HashMap<>();
        while (resultSet.next()) {
          String key = resultSet.getString(KEY_COL);
          String vResult = resultSet.getString("vresult");
          LOG.debug("Record queried: {}", vResult);
          actualResults.put(key, vResult);
        }
        return actualResults;
      } catch (DataServiceException | DataClientException e) {
        return Collections.emptyMap();
      }
    };
    return retry.executeSupplier(recordSearchSupplier);
  }


  private static class SimpleCommitter extends CheckpointCommitter {
    private static final long serialVersionUID = 1L;

    private List<Tuple2<Long, Integer>> checkpoints;

    @Override
    public void open() throws Exception {}

    @Override
    public void close() throws Exception {}

    @Override
    public void createResource() throws Exception {
      checkpoints = new ArrayList<>();
    }

    @Override
    public void commitCheckpoint(int subtaskIdx, long checkpointID) {
      checkpoints.add(new Tuple2<>(checkpointID, subtaskIdx));
    }

    @Override
    public boolean isCheckpointCommitted(int subtaskIdx, long checkpointID) {
      return checkpoints.contains(new Tuple2<>(checkpointID, subtaskIdx));
    }
  }
}
