package com.microsoft.azure.kusto;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.flink.writer.internal.sink.KustoSink;

public class KustoWriteSinkV2<IN> {
  protected static final Logger LOG = LoggerFactory.getLogger(KustoWriteSinkV2.class);

  public static class KustoSinkBuilder<IN> {
    protected TypeSerializer<IN> serializer;
    protected TypeInformation<IN> typeInfo;
    protected KustoConnectionOptions connectionOptions;
    protected KustoWriteOptions writeOptions;

    public KustoSinkBuilder() {}

    public KustoSinkBuilder<IN> setConnectionOptions(KustoConnectionOptions connectionOptions) {
      if (connectionOptions == null) {
        throw new IllegalArgumentException(
            "Connection options cannot be null. Please use KustoConnectionOptions.Builder() to create one. ");
      }
      this.connectionOptions = connectionOptions;
      return this;
    }

    public KustoSinkBuilder<IN> setWriteOptions(KustoWriteOptions writeOptions) {
      if (writeOptions == null) {
        throw new IllegalArgumentException(
            "Connection options cannot be null. Please use KustoConnectionOptions.Builder() to create one.");
      }
      this.writeOptions = writeOptions;
      return this;
    }

    protected void sanityCheck() {
      if (this.connectionOptions == null) {
        throw new IllegalArgumentException(
            "Kusto clusterUri and authentication details must be supplied through the KustoConnectionOptions.");
      }
      if (this.writeOptions == null) {
        throw new IllegalArgumentException(
            "For KustoWriteSink, the database and table to write should be passed through KustoWriteOptions.");
      }
    }

    public Sink<IN> build() throws Exception {
      sanityCheck();
      this.typeInfo = TypeInformation.of(new TypeHint<IN>() {});
      this.serializer = typeInfo.createSerializer(new ExecutionConfig());
      return new KustoSink<>(this.connectionOptions, this.writeOptions, this.serializer,
          this.typeInfo);
    }
  }
}
