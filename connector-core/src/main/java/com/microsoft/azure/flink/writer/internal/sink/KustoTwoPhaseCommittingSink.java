package com.microsoft.azure.flink.writer.internal.sink;

import java.io.IOException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Flink sink using the two-phase commit protocol for exactly-once delivery semantics.
 *
 * <p>Phase 1 (prepareCommit): The writer buffers records and uploads them to Azure Blob Storage.
 * Phase 2 (commit): The committer triggers Kusto ingestion from the uploaded blobs.
 *
 * <p>This replaces the deprecated {@code GenericWriteAheadSink}-based implementation.
 */
public class KustoTwoPhaseCommittingSink<IN>
    implements TwoPhaseCommittingSink<IN, KustoCommittable> {
  private static final Logger LOG = LoggerFactory.getLogger(KustoTwoPhaseCommittingSink.class);
  private static final long serialVersionUID = 1L;

  private final KustoConnectionOptions connectionOptions;
  private final KustoWriteOptions writeOptions;
  private final TypeSerializer<IN> serializer;
  private final TypeInformation<IN> typeInfo;

  public KustoTwoPhaseCommittingSink(KustoConnectionOptions connectionOptions,
      KustoWriteOptions writeOptions, TypeSerializer<IN> serializer, TypeInformation<IN> typeInfo) {
    this.connectionOptions = checkNotNull(connectionOptions);
    this.writeOptions = checkNotNull(writeOptions);
    this.serializer = checkNotNull(serializer);
    this.typeInfo = checkNotNull(typeInfo);
  }

  @Override
  public PrecommittingSinkWriter<IN, KustoCommittable> createWriter(WriterInitContext context)
      throws IOException {
    LOG.info("Creating KustoPrecommittingSinkWriter for DB {} in cluster {}",
        writeOptions.getDatabase(), connectionOptions.getClusterUrl());
    return new KustoPrecommittingSinkWriter<>(connectionOptions, writeOptions, serializer, typeInfo,
        context);
  }

  @Override
  public Committer<KustoCommittable> createCommitter(CommitterInitContext context)
      throws IOException {
    LOG.info("Creating KustoSinkCommitter for DB {} in cluster {}", writeOptions.getDatabase(),
        connectionOptions.getClusterUrl());
    try {
      return new KustoSinkCommitter(connectionOptions, writeOptions);
    } catch (Exception e) {
      throw new IOException("Failed to create KustoSinkCommitter", e);
    }
  }

  @Override
  public SimpleVersionedSerializer<KustoCommittable> getCommittableSerializer() {
    return new KustoCommittableSerializer();
  }
}
