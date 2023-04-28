package com.microsoft.azure.flink.flink.it;

import java.io.Serializable;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.writer.internal.KustoGenericWriteAheadSink;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import static com.microsoft.azure.flink.flink.ITSetup.getConnectorProperties;
import static com.microsoft.azure.flink.flink.ITSetup.getWriteOptions;
import static org.junit.jupiter.api.Assertions.fail;

public class KustoGenericWriteAheadSinkIT {
  private static final Logger LOG = LoggerFactory.getLogger(KustoGenericWriteAheadSinkIT.class);
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
  public void test() {
    try {
      TypeSerializer<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> serializer =
          TypeInformation.of(
              new TypeHint<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>>() {})
              .createSerializer(new ExecutionConfig());
      // TypeExtractor.getForObject(
      // new Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>())
      // .createSerializer(new ExecutionConfig());
      KustoGenericWriteAheadSink<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>> kustoGenericWriteAheadSink =
          new KustoGenericWriteAheadSink<>(coordinates, writeOptions, new SimpleCommitter(),
              serializer, UUID.randomUUID().toString());
      OneInputStreamOperatorTestHarness<Tuple8<Integer, Double, String, Boolean, Double, String, Long, String>, ?> testHarness =
          new OneInputStreamOperatorTestHarness<>(kustoGenericWriteAheadSink);
      testHarness.open();
      for (int x = 0; x < 10; x++) {
        testHarness.processElement(new StreamRecord<>(new Tuple8<>(x, 1.11 * x,
            "2019-01-01T00:00:00.000Z", true, 2.1d * x, "Flink-,;" + x, 201L * x, "flink")));
      }
      testHarness.snapshot(0, 0);
      testHarness.notifyOfCompletedCheckpoint(0);
      testHarness.close();
      assert (1 == Integer.parseInt("1"));
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

  @SuppressWarnings("unchecked")
  private static class TableObject extends Tuple implements Serializable {
    // (vnum:int, vdec:decimal, vdate:datetime, vb:boolean, vreal:real, vstr:string,
    // vlong:long,type:string)
    int vnum;
    double vdec;
    String vdate;
    boolean vb;
    double vreal;
    String vstr;
    long vlong;
    String type;
    private transient List<Object> values;

    public TableObject(int vnum, double vdec, String vdate, boolean vb, double vreal, String vstr,
        long vlong, String type) {
      this.vnum = vnum;
      this.vdec = vdec;
      this.vdate = vdate;
      this.vb = vb;
      this.vreal = vreal;
      this.vstr = vstr;
      this.vlong = vlong;
      this.type = type;
      this.values = Collections.synchronizedList(new ArrayList<>(8));
    }

    public TableObject() {

    }

    @Override
    public <T> T getField(int i) {
      return (T) this.values.get(i);
    }

    @Override
    public <T> void setField(T t, int i) {
      this.values.add(i, t);
    }

    @Override
    public int getArity() {
      return 8;
    }

    @Override
    public <T extends Tuple> T copy() {
      try {
        T t = (T) TableObject.newInstance(8);
        for (int i = 0; i < 8; i++) {
          t.setField(getField(i), i);
        }
        return t;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
