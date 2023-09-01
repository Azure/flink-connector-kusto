// package com.microsoft.azure.kusto;
//
// import com.microsoft.azure.flink.config.KustoConnectionOptions;
// import com.microsoft.azure.flink.config.KustoWriteOptions;
// import com.microsoft.azure.flink.writer.internal.committer.KustoCommitter;
// import com.microsoft.azure.flink.writer.internal.sink.KustoGenericWriteAheadSink;
// import com.microsoft.azure.flink.writer.internal.sink.KustoSink;
// import org.apache.flink.annotation.PublicEvolving;
// import org.apache.flink.api.common.typeinfo.TypeInformation;
// import org.apache.flink.api.common.typeutils.TypeSerializer;
// import org.apache.flink.api.dag.Transformation;
// import org.apache.flink.api.java.tuple.Tuple;
// import org.apache.flink.api.java.typeutils.PojoTypeInfo;
// import org.apache.flink.api.java.typeutils.RowTypeInfo;
// import org.apache.flink.api.java.typeutils.TupleTypeInfo;
// import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;
// import org.apache.flink.connector.base.DeliveryGuarantee;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.datastream.DataStreamSink;
// import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
// import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
// import org.apache.flink.types.Row;
// import org.jetbrains.annotations.NotNull;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import scala.Product;
//
// import java.util.UUID;
//
// public class KustoWriteSink<IN> {
// protected static final Logger LOG = LoggerFactory.getLogger(KustoWriteSink.class);
// private final boolean useDataStreamSink;
// private DataStreamSink<IN> sink1;
// private SingleOutputStreamOperator<IN> sink2;
//
// private KustoWriteSink(DataStreamSink<IN> sink) {
// sink1 = sink;
// useDataStreamSink = true;
// }
//
// private KustoWriteSink(SingleOutputStreamOperator<IN> sink) {
// sink2 = sink;
// useDataStreamSink = false;
// }
//
// public static <IN> @NotNull KustoSinkBuilder<IN> addSink(
// org.apache.flink.streaming.api.scala.DataStream<IN> input) {
// return addSink(input.javaStream());
// }
//
// @SuppressWarnings({"unchecked"})
// public static <IN> @NotNull KustoSinkBuilder<IN> addSink(@NotNull DataStream<IN> input) {
// TypeInformation<IN> typeInfo = input.getType();
// if (typeInfo instanceof TupleTypeInfo) {
// DataStream<Tuple> rowInput = (DataStream<Tuple>) input;
// return (KustoSinkBuilder<IN>) new KustoTupleSinkBuilder<>(rowInput,
// rowInput.getType().createSerializer(rowInput.getExecutionEnvironment().getConfig()),
// rowInput.getType());
// }
// if (typeInfo instanceof RowTypeInfo) {
// DataStream<Row> rowInput = (DataStream<Row>) input;
// return (KustoSinkBuilder<IN>) new KustoRowSinkBuilder(rowInput,
// rowInput.getType().createSerializer(rowInput.getExecutionEnvironment().getConfig()),
// rowInput.getType());
// }
// if (typeInfo instanceof CaseClassTypeInfo) {
// DataStream<Product> rowInput = (DataStream<Product>) input;
// return (KustoSinkBuilder<IN>) new KustoProductSinkBuilder<>(rowInput,
// rowInput.getType().createSerializer(rowInput.getExecutionEnvironment().getConfig()),
// rowInput.getType());
// }
// if (typeInfo instanceof PojoTypeInfo) {
// return new KustoPojoSinkBuilder<>(input,
// input.getType().createSerializer(input.getExecutionEnvironment().getConfig()),
// input.getType());
// }
// throw new IllegalArgumentException(
// "No support for the type of the given DataStream: " + input.getType());
// }
//
// private LegacySinkTransformation<IN> getSinkTransformation() {
// return sink1.getLegacyTransformation();
// }
//
// private Transformation<IN> getTransformation() {
// return sink2.getTransformation();
// }
//
// /**
// * Sets the name of this sink. This name is used by the visualization and logging during runtime.
// *
// * @return The named sink.
// */
// public KustoWriteSink<IN> name(String name) {
// if (useDataStreamSink) {
// getSinkTransformation().setName(name);
// } else {
// getTransformation().setName(name);
// }
// return this;
// }
//
// /**
// * Sets an ID for this operator.
// *
// * <p>
// * The specified ID is used to assign the same operator ID across job submissions (for example
// * when starting a job from a savepoint).
// *
// * <p>
// * <strong>Important</strong>: this ID needs to be unique per transformation and job. Otherwise,
// * job submission will fail.
// *
// * @param uid The unique user-specified ID of this transformation.
// * @return The operator with the specified ID.
// */
// @PublicEvolving
// public KustoWriteSink<IN> uid(String uid) {
// if (useDataStreamSink) {
// getSinkTransformation().setUid(uid);
// } else {
// getTransformation().setUid(uid);
// }
// return this;
// }
//
// /**
// * Sets an user provided hash for this operator. This will be used AS IS the create the
// * JobVertexID.
// *
// * <p>
// * The user provided hash is an alternative to the generated hashes, that is considered when
// * identifying an operator through the default hash mechanics fails (e.g. because of changes
// * between Flink versions).
// *
// * <p>
// * <strong>Important</strong>: this should be used as a workaround or for trouble shooting. The
// * provided hash needs to be unique per transformation and job. Otherwise, job submission will
// * fail. Furthermore, you cannot assign user-specified hash to intermediate nodes in an operator
// * chain and trying so will let your job fail.
// *
// * <p>
// * A use case for this is in migration between Flink versions or changing the jobs in a way that
// * changes the automatically generated hashes. In this case, providing the previous hashes
// * directly through this method (e.g. obtained from old logs) can help to reestablish a lost
// * mapping from states to their target operator.
// *
// * @param uidHash The user provided hash for this operator. This will become the JobVertexID,
// * which is shown in the logs and web ui.
// * @return The operator with the user provided hash.
// */
// @PublicEvolving
// public KustoWriteSink<IN> setUidHash(String uidHash) {
// if (useDataStreamSink) {
// getSinkTransformation().setUidHash(uidHash);
// } else {
// getTransformation().setUidHash(uidHash);
// }
// return this;
// }
//
// /**
// * Sets the parallelism for this sink. The degree must be higher than zero.
// *
// * @param parallelism The parallelism for this sink.
// * @return The sink with set parallelism.
// */
// public KustoWriteSink<IN> setParallelism(int parallelism) {
// if (useDataStreamSink) {
// sink1.setParallelism(parallelism);
// } else {
// sink2.setParallelism(parallelism);
// }
// return this;
// }
//
// /**
// * Turns off chaining for this operator so thread co-location will not be used as an optimization.
// *
// * <p>
// * Chaining can be turned off for the whole job by
// * {@link
// org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#disableOperatorChaining()}
// * however it is not advised for performance considerations.
// *
// * @return The sink with chaining disabled
// */
// public KustoWriteSink<IN> disableChaining() {
// if (useDataStreamSink) {
// sink1.disableChaining();
// } else {
// sink2.disableChaining();
// }
// return this;
// }
//
// /**
// * Sets the slot sharing group of this operation. Parallel instances of operations that are in the
// * same slot sharing group will be co-located in the same TaskManager slot, if possible.
// *
// * <p>
// * Operations inherit the slot sharing group of input operations if all input operations are in
// * the same slot sharing group and no slot sharing group was explicitly specified.
// *
// * <p>
// * Initially an operation is in the default slot sharing group. An operation can be put into the
// * default group explicitly by setting the slot sharing group to {@code "default"}.
// *
// * @param slotSharingGroup The slot sharing group name.
// */
// public KustoWriteSink<IN> slotSharingGroup(String slotSharingGroup) {
// if (useDataStreamSink) {
// getSinkTransformation().setSlotSharingGroup(slotSharingGroup);
// } else {
// getTransformation().setSlotSharingGroup(slotSharingGroup);
// }
// return this;
// }
//
// public static abstract class KustoSinkBuilder<IN> {
// protected final DataStream<IN> input;
// protected final TypeSerializer<IN> serializer;
// protected final TypeInformation<IN> typeInfo;
// protected KustoConnectionOptions connectionOptions;
// protected KustoWriteOptions writeOptions;
//
// public KustoSinkBuilder(DataStream<IN> input, TypeSerializer<IN> serializer,
// TypeInformation<IN> typeInfo) {
// this.input = input;
// this.serializer = serializer;
// this.typeInfo = typeInfo;
// }
//
// public KustoSinkBuilder<IN> setConnectionOptions(KustoConnectionOptions connectionOptions) {
// if (connectionOptions == null) {
// throw new IllegalArgumentException(
// "Connection options cannot be null. Please use KustoConnectionOptions.Builder() to create one.
// ");
// }
// this.connectionOptions = connectionOptions;
// return this;
// }
//
// public KustoSinkBuilder<IN> setWriteOptions(KustoWriteOptions writeOptions) {
// if (writeOptions == null) {
// throw new IllegalArgumentException(
// "Connection options cannot be null. Please use KustoConnectionOptions.Builder() to create one.");
// }
// this.writeOptions = writeOptions;
// return this;
// }
//
// protected abstract KustoWriteSink<IN> createWriteAheadSink() throws Exception;
//
// protected abstract KustoWriteSink<IN> createSink() throws Exception;
//
// protected void sanityCheck() {
// if (this.connectionOptions == null) {
// throw new IllegalArgumentException(
// "Kusto clusterUri and authentication details must be supplied through the
// KustoConnectionOptions.");
// }
// if (this.writeOptions == null) {
// throw new IllegalArgumentException(
// "For KustoWriteSink, the database and table to write should be passed through
// KustoWriteOptions.");
// }
// }
//
// public KustoWriteSink<IN> build() throws Exception {
// sanityCheck();
// if (writeOptions.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
// LOG.info("Creating KustoWriteAheadSink with at-least once guarantee.");
// return createSink();
// } else {
// LOG.info("Creating KustoWriteAheadSink with at-least once guarantee. Checkpoints will be "
// + "performed in Kusto tables and will have some performance implications");
// return createWriteAheadSink();
// }
// }
// }
//
// public static class KustoRowSinkBuilder extends KustoSinkBuilder<Row> {
// public KustoRowSinkBuilder(DataStream<Row> input, TypeSerializer<Row> serializer,
// TypeInformation<Row> typeInfo) {
// super(input, serializer, typeInfo);
// }
//
// @Override
// protected KustoWriteSink<Row> createWriteAheadSink() throws Exception {
// final KustoGenericWriteAheadSink<Row> kustoGenericWriteAheadSink =
// new KustoGenericWriteAheadSink<>(this.connectionOptions, this.writeOptions,
// new KustoCommitter(this.connectionOptions, this.writeOptions), this.serializer,
// this.typeInfo, UUID.randomUUID().toString());
// return new KustoWriteSink<>(
// input.transform("Kusto Row Sink", null, kustoGenericWriteAheadSink));
// }
//
// @Override
// protected KustoWriteSink<Row> createSink() throws Exception {
// final KustoSink<Row> kustoSink = new KustoSink<>(this.connectionOptions, this.writeOptions,
// this.serializer, this.typeInfo);
// return new KustoWriteSink<>(input.sinkTo(kustoSink).name("Kusto Row Sink"));
// }
// }
//
// public static class KustoTupleSinkBuilder<IN extends Tuple> extends KustoSinkBuilder<IN> {
// public KustoTupleSinkBuilder(DataStream<IN> input, TypeSerializer<IN> serializer,
// TypeInformation<IN> typeInfo) {
// super(input, serializer, typeInfo);
// }
//
// @Override
// protected KustoWriteSink<IN> createWriteAheadSink() throws Exception {
// final KustoGenericWriteAheadSink<IN> kustoGenericWriteAheadSink =
// new KustoGenericWriteAheadSink<>(this.connectionOptions, this.writeOptions,
// new KustoCommitter(this.connectionOptions, this.writeOptions), this.serializer,
// this.typeInfo, UUID.randomUUID().toString());
// return new KustoWriteSink<>(
// input.transform("Kusto Tuple Sink", null, kustoGenericWriteAheadSink));
// }
//
// @Override
// protected KustoWriteSink<IN> createSink() throws Exception {
// final KustoSink<IN> kustoSink = new KustoSink<>(this.connectionOptions, this.writeOptions,
// this.serializer, this.typeInfo);
// LOG.info("Writing a tuple sink to DB {} in cluster {} ", writeOptions.getDatabase(),
// connectionOptions.getClusterUrl());
// return new KustoWriteSink<>(input.sinkTo(kustoSink).name("Kusto Tuple Sink"));
// }
// }
//
// public static class KustoProductSinkBuilder<IN extends Product> extends KustoSinkBuilder<IN> {
// public KustoProductSinkBuilder(DataStream<IN> input, TypeSerializer<IN> serializer,
// TypeInformation<IN> typeInfo) {
// super(input, serializer, typeInfo);
// }
//
// @Override
// protected KustoWriteSink<IN> createWriteAheadSink() throws Exception {
// return new KustoWriteSink<>(input.transform("Kusto Product Sink", null,
// new KustoGenericWriteAheadSink<>(this.connectionOptions, this.writeOptions,
// new KustoCommitter(this.connectionOptions, this.writeOptions), this.serializer,
// this.typeInfo, UUID.randomUUID().toString())));
// }
//
// @Override
// protected KustoWriteSink<IN> createSink() throws Exception {
// final KustoSink<IN> kustoSink = new KustoSink<>(this.connectionOptions, this.writeOptions,
// this.serializer, this.typeInfo);
// return new KustoWriteSink<>(input.sinkTo(kustoSink).name("Kusto Product Sink"));
// }
// }
//
// public static class KustoPojoSinkBuilder<IN> extends KustoSinkBuilder<IN> {
// public KustoPojoSinkBuilder(DataStream<IN> input, TypeSerializer<IN> serializer,
// TypeInformation<IN> typeInfo) {
// super(input, serializer, typeInfo);
// }
//
// @Override
// protected KustoWriteSink<IN> createWriteAheadSink() throws Exception {
// return new KustoWriteSink<>(input.transform("Kusto Pojo Sink", null,
// new KustoGenericWriteAheadSink<>(this.connectionOptions, this.writeOptions,
// new KustoCommitter(this.connectionOptions, this.writeOptions), this.serializer,
// this.typeInfo, UUID.randomUUID().toString())));
// }
//
// @Override
// protected KustoWriteSink<IN> createSink() throws Exception {
// final KustoSink<IN> kustoSink = new KustoSink<>(this.connectionOptions, this.writeOptions,
// this.serializer, this.typeInfo);
// return new KustoWriteSink<>(input.sinkTo(kustoSink).name("Kusto Pojo Sink"));
// }
// }
// }
