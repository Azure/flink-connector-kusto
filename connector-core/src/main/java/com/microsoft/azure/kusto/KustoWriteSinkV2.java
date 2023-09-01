package com.microsoft.azure.kusto;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.writer.internal.sink.KustoSink;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KustoWriteSinkV2 {
  protected static final Logger LOG = LoggerFactory.getLogger(KustoWriteSinkV2.class);
  protected KustoConnectionOptions connectionOptions;
  protected KustoWriteOptions writeOptions;

  private KustoWriteSinkV2() {}

  @Contract(" -> new")
  public static @NotNull KustoWriteSinkV2 builder() {
    return new KustoWriteSinkV2();
  }

  public KustoWriteSinkV2 setConnectionOptions(KustoConnectionOptions connectionOptions) {
    if (connectionOptions == null) {
      throw new IllegalArgumentException(
          "Connection options cannot be null. Please use KustoConnectionOptions.Builder() to create one. ");
    }
    this.connectionOptions = connectionOptions;
    return this;
  }

  public KustoWriteSinkV2 setWriteOptions(KustoWriteOptions writeOptions) {
    if (writeOptions == null) {
      throw new IllegalArgumentException(
          "Connection options cannot be null. Please use KustoConnectionOptions.Builder() to create one.");
    }
    this.writeOptions = writeOptions;
    return this;
  }

  protected void sanityCheck() {
    checkNotNull(this.connectionOptions, "Kusto connection options must be supplied.");
    checkNotNull(this.writeOptions, "Kusto write options must be supplied.");
  }

  public <IN> void build(@NotNull DataStream<IN> dataStream) throws Exception {
    build(dataStream, 1);
  }

  public <IN> void build(@NotNull DataStream<IN> dataStream, int parallelism) throws Exception {
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
    dataStream
        .sinkTo(new KustoSink<>(this.connectionOptions, this.writeOptions, serializer, typeInfo))
        .setParallelism(parallelism);
  }

  public <IN> void buildWriteAheadSink(DataStream<IN> dataStream) throws Exception {
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
    // dataStream.addSink(new KustoGenericWriteAheadSink<>(this.connectionOptions,
    // this.writeOptions,
    // new KustoCommitter(this.connectionOptions, this.writeOptions), serializer, typeInfo,
    // UUID.randomUUID().toString()));
  }
}

