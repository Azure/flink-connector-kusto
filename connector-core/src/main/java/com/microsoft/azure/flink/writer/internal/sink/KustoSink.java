package com.microsoft.azure.flink.writer.internal.sink;

import java.net.URISyntaxException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.ClosureCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;

import static org.apache.flink.util.Preconditions.checkNotNull;

/*

 */
public class KustoSink<IN> implements Sink<IN> {
  protected static final Logger LOG = LoggerFactory.getLogger(KustoSink.class);

  private static final long serialVersionUID = 1L;
  private final KustoConnectionOptions connectionOptions;
  private final KustoWriteOptions writeOptions;
  private final TypeSerializer<IN> serializer;
  private final TypeInformation<IN> typeInfo;

  public KustoSink(KustoConnectionOptions connectionOptions, KustoWriteOptions writeOptions,
      TypeSerializer<IN> serializer, TypeInformation<IN> typeInfo) {
    this.connectionOptions = checkNotNull(connectionOptions);
    this.writeOptions = checkNotNull(writeOptions);
    this.serializer = checkNotNull(serializer);
    this.typeInfo = checkNotNull(typeInfo);
    ClosureCleaner.clean(serializer, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
  }

  @Override
  public SinkWriter<IN> createWriter(InitContext context) {
    try {
      LOG.info("Writing to DB {} in cluster {} ", writeOptions.getDatabase(),
          connectionOptions.getClusterUrl());
      return new KustoSinkWriter<>(connectionOptions, writeOptions, serializer, typeInfo, true,
          context);
    } catch (URISyntaxException e) {
      LOG.error("Writing to DB {} in cluster {} failed with URISyntaxException",
          writeOptions.getDatabase(), connectionOptions.getClusterUrl());
      throw new RuntimeException(e);
    }
  }
}
