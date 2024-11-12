package com.microsoft.azure.flink.it;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.api.scala.typeutils.ScalaCaseClassSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.TupleTestObject;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.writer.internal.committer.KustoCommitter;
import com.microsoft.azure.flink.writer.internal.sink.KustoGenericWriteAheadSink;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import scala.Product;

import static com.microsoft.azure.flink.it.ITSetup.getConnectorProperties;
import static com.microsoft.azure.flink.it.ITSetup.getWriteOptions;

@SuppressWarnings({"rawtypes", "unchecked"})
@Execution(ExecutionMode.CONCURRENT)
public class KustoGenericWriteAheadSinkIT {
  private static final Logger LOG = LoggerFactory.getLogger(KustoGenericWriteAheadSinkIT.class);
  private static Client engineClient;
  private static Client dmClient;
  private static KustoConnectionOptions coordinates;
  private static KustoWriteOptions writeOptions;

  @BeforeAll
  public static void setUp() {
    ConnectionStringBuilder engineCsb;
    ConnectionStringBuilder dmCsb;
    coordinates = getConnectorProperties();
    writeOptions = getWriteOptions();
    coordinates = getConnectorProperties();
    if (StringUtils.isNotEmpty(coordinates.getAppId())
        && StringUtils.isNotEmpty(coordinates.getAppKey())
        && StringUtils.isNotEmpty(coordinates.getTenantId())
        && StringUtils.isNotEmpty(coordinates.getClusterUrl())) {
      LOG.info("Connecting to cluster: {}", coordinates.getClusterUrl());
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
    LOG.info("Finished table clean up. Dropped table {}", writeOptions.getTable());
    dmClient.close();
    engineClient.close();
  }

  @Test
  public void testTupleIngest() throws Exception {
    String typeKey = "FlinkTupleTest";
    TypeInformation<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> typeInfo =
        TypeInformation.of(
            new TypeHint<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>>() {});
    TypeSerializer<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> serializer =
        typeInfo.createSerializer(new SerializerConfigImpl());
    KustoCommitter kustoCommitter = new KustoCommitter(coordinates, writeOptions);
    kustoCommitter.open();
    KustoGenericWriteAheadSink<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> kustoGenericWriteAheadSink =
        new KustoGenericWriteAheadSink<>(coordinates, writeOptions, kustoCommitter, serializer,
            typeInfo, UUID.randomUUID().toString());
    OneInputStreamOperatorTestHarness<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>, ?> testHarness =
        new OneInputStreamOperatorTestHarness<>(kustoGenericWriteAheadSink);
    int maxRecords = 100;
    Map<String, String> expectedResults = new HashMap<>();
    testHarness.open();
    for (int x = 0; x < maxRecords; x++) {
      TupleTestObject tupleTestObject = new TupleTestObject(x, typeKey);
      testHarness.processElement(new StreamRecord(tupleTestObject.toTuple()));
      LOG.debug("Processed tuple with key {}", tupleTestObject.getVstr());
      expectedResults.put(tupleTestObject.getVstr(), tupleTestObject.toJsonString());
    }
    performTest(testHarness, expectedResults, maxRecords, typeKey);
  }

  @Test
  public void testRowIngest() throws Exception {
    String typeKey = "FlinkRowTest";
    TypeInformation<Row> rowTypeInformation = TypeInformation.of(new TypeHint<Row>() {});
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
      fieldSerializers[i] = types[i].createSerializer(new SerializerConfigImpl());
    }
    RowSerializer rowSerializer = new RowSerializer(fieldSerializers);
    KustoCommitter kustoCommitter = new KustoCommitter(coordinates, writeOptions);
    kustoCommitter.open();
    KustoGenericWriteAheadSink<Row> kustoGenericWriteAheadSink =
        new KustoGenericWriteAheadSink<>(coordinates, writeOptions, kustoCommitter, rowSerializer,
            rowTypeInformation, UUID.randomUUID().toString());
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
    TypeInformation<Product> productTypeInformation =
        TypeInformation.of(new TypeHint<Product>() {});
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
      fieldSerializers[i] = types[i].createSerializer(new SerializerConfigImpl());
    }
    KustoCommitter kustoCommitter = new KustoCommitter(coordinates, writeOptions);
    kustoCommitter.open();
    KustoGenericWriteAheadSink<Row> kustoGenericWriteAheadSink =
        new KustoGenericWriteAheadSink<>(coordinates, writeOptions, kustoCommitter,
            new ScalaCaseClassSerializer(scala.Tuple8.class, fieldSerializers),
            productTypeInformation, UUID.randomUUID().toString());
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

  @Test
  public void testPojoIngest() throws Exception {
    String typeKey = "FlinkPojoTest";
    TypeInformation<TupleTestObject> typeInfo = TypeInformation.of(TupleTestObject.class);
    PojoTypeInfo<TupleTestObject> pojoTypeInfo = (PojoTypeInfo<TupleTestObject>) typeInfo;
    KustoCommitter kustoCommitter = new KustoCommitter(coordinates, writeOptions);
    kustoCommitter.open();
    KustoGenericWriteAheadSink<TupleTestObject> kustoGenericWriteAheadSink =
        new KustoGenericWriteAheadSink<>(coordinates, writeOptions, kustoCommitter,
            pojoTypeInfo.createSerializer(new SerializerConfigImpl()), pojoTypeInfo,
            UUID.randomUUID().toString());
    OneInputStreamOperatorTestHarness<TupleTestObject, ?> testHarness =
        new OneInputStreamOperatorTestHarness<>(kustoGenericWriteAheadSink);
    int maxRecords = 100;
    Map<String, String> expectedResults = new HashMap<>();
    testHarness.open();
    for (int x = 0; x < maxRecords; x++) {
      TupleTestObject tupleTestObject = new TupleTestObject(x, typeKey);
      testHarness.processElement(new StreamRecord(tupleTestObject));
      expectedResults.put(tupleTestObject.getVstr(), tupleTestObject.toJsonString());
    }
    performTest(testHarness, expectedResults, maxRecords, typeKey);
  }

  private static void performTest(@NotNull OneInputStreamOperatorTestHarness<?, ?> testHarness,
      Map<String, String> expectedResults, int maxRecords, String typeKey) throws Exception {
    long checkpointId = Instant.now(Clock.systemUTC()).toEpochMilli();
    testHarness.snapshot(checkpointId, Instant.now(Clock.systemUTC()).toEpochMilli());
    testHarness.notifyOfCompletedCheckpoint(checkpointId);
    testHarness.close();
    KustoTestUtil.performAssertions(engineClient, writeOptions, expectedResults, maxRecords,
        typeKey);
  }
}
