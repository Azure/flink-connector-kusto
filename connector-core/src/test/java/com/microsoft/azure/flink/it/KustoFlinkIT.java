// package com.microsoft.azure.flink.it;
//
// import org.apache.commons.lang3.StringUtils;
// import org.apache.flink.api.common.restartstrategy.RestartStrategies;
// import org.apache.flink.connector.base.DeliveryGuarantee;
// import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.flink.test.junit5.MiniClusterExtension;
// import org.junit.jupiter.api.AfterAll;
// import org.junit.jupiter.api.BeforeAll;
// import org.junit.jupiter.api.extension.RegisterExtension;
// import org.junit.jupiter.params.ParameterizedTest;
// import org.junit.jupiter.params.provider.EnumSource;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import com.microsoft.azure.flink.TestSinkInitContext;
// import com.microsoft.azure.flink.config.KustoConnectionOptions;
// import com.microsoft.azure.flink.config.KustoWriteOptions;
// import com.microsoft.azure.kusto.data.Client;
// import com.microsoft.azure.kusto.data.ClientFactory;
// import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
//
// import static com.microsoft.azure.flink.it.ITSetup.getConnectorProperties;
// import static com.microsoft.azure.flink.it.ITSetup.getWriteOptions;
//
/// ** IT cases for {@link com.microsoft.azure.kusto.KustoWriteSink}. */
// public class KustoFlinkIT {
// private static final Logger LOG = LoggerFactory.getLogger(KustoFlinkIT.class);
//
// private static Client engineClient;
// private static Client dmClient;
// private static KustoConnectionOptions coordinates;
// private static KustoWriteOptions writeOptions;
//
// @RegisterExtension
// static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension(
// new MiniClusterResourceConfiguration.Builder().setNumberTaskManagers(1).build());
//
// @BeforeAll
// public static void setUp() {
// coordinates = getConnectorProperties();
// writeOptions = getWriteOptions();
// coordinates = getConnectorProperties();
// if (StringUtils.isNotEmpty(coordinates.getAppId())
// && StringUtils.isNotEmpty(coordinates.getAppKey())
// && StringUtils.isNotEmpty(coordinates.getTenantId())
// && StringUtils.isNotEmpty(coordinates.getClusterUrl())) {
// LOG.error("Connecting to cluster: {}", coordinates.getClusterUrl());
// ConnectionStringBuilder engineCsb =
// ConnectionStringBuilder.createWithAadApplicationCredentials(coordinates.getClusterUrl(),
// coordinates.getAppId(), coordinates.getAppKey(), coordinates.getTenantId());
// ConnectionStringBuilder dmCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
// coordinates.getClusterUrl().replaceAll("https://", "https://ingest-"),
// coordinates.getAppId(), coordinates.getAppKey(), coordinates.getTenantId());
// try {
// engineClient = ClientFactory.createClient(engineCsb);
// dmClient = ClientFactory.createClient(dmCsb);
// LOG.info("Creating tables in Kusto");
// KustoTestUtil.createTables(engineClient, writeOptions);
// KustoTestUtil.refreshDm(dmClient, writeOptions);
// } catch (Exception e) {
// throw new RuntimeException(e);
// }
// } else {
// LOG.info("Skipping test due to missing configuration");
// }
// }
//
// @ParameterizedTest
// @EnumSource(
// value = DeliveryGuarantee.class,
// mode = EnumSource.Mode.EXCLUDE,
// names = "EXACTLY_ONCE")
// void testWriteToMongoWithDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee)
// throws Exception {
// final String collection = "test-sink-with-delivery-" + deliveryGuarantee;
// //final MongoSink<Document> sink = createSink(collection, deliveryGuarantee);
// final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// env.enableCheckpointing(100L);
// env.setRestartStrategy(RestartStrategies.noRestart());
//
// env.fromSequence(1, 5).map(new TestMapFunction()).sinkTo(sink);
// env.execute();
// }
//
// @AfterAll
// public static void tearDown() throws Exception {
// engineClient.execute(writeOptions.getDatabase(),
// String.format(".drop table %s", writeOptions.getTable()));
// LOG.error("Finished table clean up. Dropped table {}", writeOptions.getTable());
// dmClient.close();
// engineClient.close();
// }
// }
