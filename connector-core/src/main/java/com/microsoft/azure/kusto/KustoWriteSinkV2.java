package com.microsoft.azure.kusto;

import java.util.UUID;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.writer.internal.committer.KustoCommitter;
import com.microsoft.azure.flink.writer.internal.sink.KustoGenericWriteAheadSink;
import com.microsoft.azure.flink.writer.internal.sink.KustoSink;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KustoWriteSinkV2<IN> {
  protected static final Logger LOG = LoggerFactory.getLogger(KustoWriteSinkV2.class);
  protected TypeSerializer<IN> serializer;
  protected TypeInformation<IN> typeInfo;
  protected KustoConnectionOptions connectionOptions;
  protected KustoWriteOptions writeOptions;

  private KustoWriteSinkV2() {
    this.typeInfo = TypeInformation.of(new TypeHint<IN>() {});
    this.serializer = this.typeInfo.createSerializer(new ExecutionConfig());;
  }

  @Contract(" -> new")
  public static <IN> @NotNull KustoWriteSinkV2<IN> builder() {
    return new KustoWriteSinkV2<>();
  }

  public KustoWriteSinkV2<IN> setConnectionOptions(KustoConnectionOptions connectionOptions) {
    if (connectionOptions == null) {
      throw new IllegalArgumentException(
          "Connection options cannot be null. Please use KustoConnectionOptions.Builder() to create one. ");
    }
    this.connectionOptions = connectionOptions;
    return this;
  }

  public KustoWriteSinkV2<IN> setWriteOptions(KustoWriteOptions writeOptions) {
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
    checkNotNull(this.serializer, "Kusto serializer must not be null.");
    checkNotNull(this.typeInfo, "Kusto type information must not be null.");
  }

  public Sink<IN> build() throws Exception {
    sanityCheck();
    LOG.info("Building KustoSink with WriteOptions: {} and ConnectionOptions {}", this.writeOptions.toString(),
        this.connectionOptions.toString());
    return new KustoSink<>(this.connectionOptions, this.writeOptions, this.serializer,
        this.typeInfo);
  }

  public GenericWriteAheadSink<IN> buildWriteAheadSink() throws Exception {
    sanityCheck();
    LOG.info("Building GenericWriteAheadSink with WriteOptions: {} and ConnectionOptions {}", this.writeOptions.toString(),
            this.connectionOptions.toString());
    return new KustoGenericWriteAheadSink<>(this.connectionOptions, this.writeOptions,
        new KustoCommitter(this.connectionOptions, this.writeOptions), this.serializer,
        this.typeInfo, UUID.randomUUID().toString());
  }
}

