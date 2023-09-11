package com.microsoft.azure.kusto;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.writer.internal.committer.KustoCommitter;
import com.microsoft.azure.flink.writer.internal.sink.KustoGenericWriteAheadSink;
import com.microsoft.azure.flink.writer.internal.sink.KustoSink;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@PublicEvolving
public class KustoWriteSink {
  protected static final Logger LOG = LoggerFactory.getLogger(KustoWriteSink.class);
  protected KustoConnectionOptions connectionOptions;
  protected KustoWriteOptions writeOptions;

  private KustoWriteSink() {}

  @Contract(" -> new")
  public static @NotNull KustoWriteSink builder() {
    return new KustoWriteSink();
  }

  /**
   * Sets the connection options for the Kusto cluster.
   * 
   * @param connectionOptions The connection options for the Kusto cluster.
   * @return The KustoWriteSink object.
   */
  public KustoWriteSink setConnectionOptions(KustoConnectionOptions connectionOptions) {
    if (connectionOptions == null) {
      throw new IllegalArgumentException(
          "Connection options cannot be null. Please use KustoConnectionOptions.Builder() to create one. ");
    }
    this.connectionOptions = connectionOptions;
    return this;
  }

  /**
   * Sets the write options for the Kusto cluster.
   * 
   * @param writeOptions The write options for the Kusto cluster.
   * @return The KustoWriteSink object.
   */
  public KustoWriteSink setWriteOptions(KustoWriteOptions writeOptions) {
    if (writeOptions == null) {
      throw new IllegalArgumentException(
          "Connection options cannot be null. Please use KustoConnectionOptions.Builder() to create one.");
    }
    this.writeOptions = writeOptions;
    return this;
  }

  /**
   * Sanity check for the KustoWriteSink object.
   */
  protected void sanityCheck() {
    checkNotNull(this.connectionOptions, "Kusto connection options must be supplied.");
    checkArgument(
        StringUtils.isNotEmpty(this.connectionOptions.getAppId())
            || StringUtils.isNotEmpty(this.connectionOptions.getManagedIdentityAppId()),
        "Either AppId or ManagedIdentityAppId must be supplied.");
    checkNotNull(this.writeOptions, "Kusto write options must be supplied.");
    checkNotNull(this.writeOptions.getDatabase(),
        "Kusto write options should have database name specified");
    checkNotNull(this.writeOptions.getTable(), "Kusto write options should have table specified");
  }

  public <IN> void build(@NotNull DataStream<IN> dataStream) throws Exception {
    build(dataStream, 1);
  }

  public <IN> void build(@NotNull DataStream<IN> dataStream, int parallelism) throws Exception {
    sinkDataStream(dataStream, parallelism);
  }

  private <IN> DataStreamSink<IN> sinkDataStream(@NotNull DataStream<IN> dataStream,
      int parallelism) {
    TypeInformation<IN> typeInfo = dataStream.getType();
    TypeSerializer<IN> serializer =
        typeInfo.createSerializer(dataStream.getExecutionEnvironment().getConfig());
    boolean isSupportedType = typeInfo instanceof TupleTypeInfo || typeInfo instanceof RowTypeInfo
        || typeInfo instanceof CaseClassTypeInfo || typeInfo instanceof PojoTypeInfo;
    if (!isSupportedType) {
      throw new IllegalArgumentException(
          "No support for the type of the given DataStream: " + dataStream.getType());
    }
    sanityCheck();
    LOG.info("Building KustoSink with WriteOptions: {} and ConnectionOptions {}",
        this.writeOptions.toString(), this.connectionOptions.toString());
    return dataStream
        .sinkTo(new KustoSink<>(this.connectionOptions, this.writeOptions, serializer, typeInfo))
        .setParallelism(parallelism);
  }

  public <IN> void build(@NotNull DataStream<IN> dataStream, int parallelism, String name,
      String uid) throws Exception {
    DataStreamSink<IN> sinkStream = sinkDataStream(dataStream, parallelism);
    sinkStream.name(name).uid(uid);
  }

  public <IN> void buildWriteAheadSink(@NotNull DataStream<IN> dataStream) throws Exception {
    buildWriteAheadSink(dataStream, 1);
  }

  public <IN> void buildWriteAheadSink(@NotNull DataStream<IN> dataStream, int parallelism)
      throws Exception {
    TypeInformation<IN> typeInfo = dataStream.getType();
    TypeSerializer<IN> serializer =
        typeInfo.createSerializer(dataStream.getExecutionEnvironment().getConfig());
    boolean isSupportedType = typeInfo instanceof TupleTypeInfo || typeInfo instanceof RowTypeInfo
        || typeInfo instanceof CaseClassTypeInfo || typeInfo instanceof PojoTypeInfo;
    if (!isSupportedType) {
      throw new IllegalArgumentException(
          "No support for the type of the given DataStream: " + dataStream.getType());
    }
    sanityCheck();
    LOG.info("Building GenericWriteAheadSink with WriteOptions: {} and ConnectionOptions {}",
        this.writeOptions.toString(), this.connectionOptions.toString());
    KustoGenericWriteAheadSink<IN> genericSink =
        new KustoGenericWriteAheadSink<>(this.connectionOptions, this.writeOptions,
            new KustoCommitter(this.connectionOptions, this.writeOptions), serializer, typeInfo,
            UUID.randomUUID().toString());
    String operatorName = String.format("KustoGenericWriteAheadSink-%s-%s",
        this.writeOptions.getDatabase(), this.writeOptions.getTable());
    dataStream.transform(operatorName, typeInfo, genericSink).setParallelism(parallelism);
  }
}

