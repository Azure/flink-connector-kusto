package com.microsoft.azure.flink.writer.internal.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.jetbrains.annotations.NotNull;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;

@PublicEvolving
@Internal
public class KustoGenericWriteAheadSink<IN> extends GenericWriteAheadSink<IN> {
  private final KustoConnectionOptions connectionOptions;
  private final KustoWriteOptions writeOptions;
  private KustoSinkCommon<IN> kustoSinkCommon;
  private final TypeSerializer<IN> serializer;
  private final TypeInformation<IN> typeInformation;

  /**
   * Constructor for KustoGenericWriteAheadSink.
   *
   * @param connectionOptions The connection options for the Kusto cluster.
   * @param writeOptions The write options for the Kusto cluster.
   * @param committer The committer for the checkpoint.
   * @param serializer The serializer for the data.
   * @param jobID The job ID.
   * @throws Exception If the connection options are invalid.
   */
  public KustoGenericWriteAheadSink(KustoConnectionOptions connectionOptions,
      KustoWriteOptions writeOptions, CheckpointCommitter committer, TypeSerializer<IN> serializer,
      TypeInformation<IN> typeInformation, String jobID) throws Exception {
    super(committer, serializer, jobID);
    this.connectionOptions = connectionOptions;
    this.writeOptions = writeOptions;
    this.serializer = serializer;
    this.typeInformation = typeInformation;
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!getRuntimeContext().isCheckpointingEnabled()) {
      throw new IllegalStateException("The write-ahead log requires checkpointing to be enabled.");
    }
    this.kustoSinkCommon = new KustoSinkCommon<>(this.connectionOptions, this.writeOptions,
        super.getRuntimeContext().getMetricGroup(), this.serializer, this.typeInformation);
  }

  @Override
  protected boolean sendValues(@NotNull Iterable<IN> values, long checkpointId, long timestamp) {
    return this.kustoSinkCommon.ingest(values);
  }

  @Override
  public void close() throws Exception {
    super.close();
    this.kustoSinkCommon.close();
  }
}
