package com.microsoft.azure.kusto;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.types.Row;

import com.microsoft.azure.flink.config.KustoConnectionOptions;

public class KustoSink<IN> {
  private final boolean useDataStreamSink;
  private DataStreamSink<IN> sink1;
  private SingleOutputStreamOperator<IN> sink2;

  private KustoSink(DataStreamSink<IN> sink) {
    sink1 = sink;
    useDataStreamSink = true;
  }

  private KustoSink(SingleOutputStreamOperator<IN> sink) {
    sink2 = sink;
    useDataStreamSink = false;
  }

  private LegacySinkTransformation<IN> getSinkTransformation() {
    return sink1.getLegacyTransformation();
  }

  private Transformation<IN> getTransformation() {
    return sink2.getTransformation();
  }

  /**
   * Sets the name of this sink. This name is used by the visualization and logging during runtime.
   *
   * @return The named sink.
   */
  public KustoSink<IN> name(String name) {
    if (useDataStreamSink) {
      getSinkTransformation().setName(name);
    } else {
      getTransformation().setName(name);
    }
    return this;
  }

  /**
   * Sets an ID for this operator.
   *
   * <p>
   * The specified ID is used to assign the same operator ID across job submissions (for example
   * when starting a job from a savepoint).
   *
   * <p>
   * <strong>Important</strong>: this ID needs to be unique per transformation and job. Otherwise,
   * job submission will fail.
   *
   * @param uid The unique user-specified ID of this transformation.
   * @return The operator with the specified ID.
   */
  @PublicEvolving
  public KustoSink<IN> uid(String uid) {
    if (useDataStreamSink) {
      getSinkTransformation().setUid(uid);
    } else {
      getTransformation().setUid(uid);
    }
    return this;
  }

  /**
   * Sets an user provided hash for this operator. This will be used AS IS the create the
   * JobVertexID.
   *
   * <p>
   * The user provided hash is an alternative to the generated hashes, that is considered when
   * identifying an operator through the default hash mechanics fails (e.g. because of changes
   * between Flink versions).
   *
   * <p>
   * <strong>Important</strong>: this should be used as a workaround or for trouble shooting. The
   * provided hash needs to be unique per transformation and job. Otherwise, job submission will
   * fail. Furthermore, you cannot assign user-specified hash to intermediate nodes in an operator
   * chain and trying so will let your job fail.
   *
   * <p>
   * A use case for this is in migration between Flink versions or changing the jobs in a way that
   * changes the automatically generated hashes. In this case, providing the previous hashes
   * directly through this method (e.g. obtained from old logs) can help to reestablish a lost
   * mapping from states to their target operator.
   *
   * @param uidHash The user provided hash for this operator. This will become the JobVertexID,
   *        which is shown in the logs and web ui.
   * @return The operator with the user provided hash.
   */
  @PublicEvolving
  public KustoSink<IN> setUidHash(String uidHash) {
    if (useDataStreamSink) {
      getSinkTransformation().setUidHash(uidHash);
    } else {
      getTransformation().setUidHash(uidHash);
    }
    return this;
  }

  /**
   * Sets the parallelism for this sink. The degree must be higher than zero.
   *
   * @param parallelism The parallelism for this sink.
   * @return The sink with set parallelism.
   */
  public KustoSink<IN> setParallelism(int parallelism) {
    if (useDataStreamSink) {
      sink1.setParallelism(parallelism);
    } else {
      sink2.setParallelism(parallelism);
    }
    return this;
  }

  /**
   * Turns off chaining for this operator so thread co-location will not be used as an optimization.
   *
   * <p>
   * Chaining can be turned off for the whole job by
   * {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#disableOperatorChaining()}
   * however it is not advised for performance considerations.
   *
   * @return The sink with chaining disabled
   */
  public KustoSink<IN> disableChaining() {
    if (useDataStreamSink) {
      sink1.disableChaining();
    } else {
      sink2.disableChaining();
    }
    return this;
  }

  /**
   * Sets the slot sharing group of this operation. Parallel instances of operations that are in the
   * same slot sharing group will be co-located in the same TaskManager slot, if possible.
   *
   * <p>
   * Operations inherit the slot sharing group of input operations if all input operations are in
   * the same slot sharing group and no slot sharing group was explicitly specified.
   *
   * <p>
   * Initially an operation is in the default slot sharing group. An operation can be put into the
   * default group explicitly by setting the slot sharing group to {@code "default"}.
   *
   * @param slotSharingGroup The slot sharing group name.
   */
  public KustoSink<IN> slotSharingGroup(String slotSharingGroup) {
    if (useDataStreamSink) {
      getSinkTransformation().setSlotSharingGroup(slotSharingGroup);
    } else {
      getTransformation().setSlotSharingGroup(slotSharingGroup);
    }
    return this;
  }

  public static <IN> KustoSinkBuilder<IN> addSink(
      org.apache.flink.streaming.api.scala.DataStream<IN> input) {
    return addSink(input.javaStream());
  }

  public static <IN> KustoSinkBuilder<IN> addSink(DataStream<IN> input) {
    TypeInformation<IN> typeInfo = input.getType();
    return null;
  }

  public static abstract class KustoSinkBuilder<IN> {
    protected KustoConnectionOptions.Builder connectionOptionsBuilder;

    protected final DataStream<IN> input;
    protected final TypeSerializer<IN> serializer;
    protected final TypeInformation<IN> typeInfo;


    public KustoSinkBuilder(DataStream<IN> input, TypeSerializer<IN> serializer,
        TypeInformation<IN> typeInfo) {
      this.connectionOptionsBuilder = KustoConnectionOptions.builder();
      this.input = input;
      this.serializer = serializer;
      this.typeInfo = typeInfo;
    }

    public KustoSinkBuilder<IN> setConnectionOptions(
        KustoConnectionOptions.Builder connectionOptionsBuilder) {
      if (this.connectionOptionsBuilder != null) {
        throw new IllegalArgumentException(
            "Builder was already set. You must use either setHost() or setClusterBuilder().");
      }
      this.connectionOptionsBuilder = connectionOptionsBuilder;
      return this;
    }

    public KustoSinkBuilder<IN> setCluster(String clusterUrl) {
      this.connectionOptionsBuilder.setClusterUrl(clusterUrl);
      return this;
    }

    public KustoSinkBuilder<IN> setAppId(String appId) {
      this.connectionOptionsBuilder.setAppId(appId);
      return this;
    }

    public KustoSinkBuilder<IN> setAppKey(String appKey) {
      this.connectionOptionsBuilder.setAppKey(appKey);
      return this;
    }

    public KustoSinkBuilder<IN> setTenantId(String tenantId) {
      this.connectionOptionsBuilder.setTenantId(tenantId);
      return this;
    }

    public KustoSinkBuilder<IN> setManagedIdentityAppId(String managedIdentityAppId) {
      this.connectionOptionsBuilder.setManagedIdentityAppId(managedIdentityAppId);
      return this;
    }
  }

  public static class KustoRowSinkBuilder extends KustoSinkBuilder<Row> {

    public KustoRowSinkBuilder(DataStream<Row> input, TypeSerializer<Row> serializer,
        TypeInformation<Row> typeInfo) {
      super(input, serializer, typeInfo);
    }
  }
}
