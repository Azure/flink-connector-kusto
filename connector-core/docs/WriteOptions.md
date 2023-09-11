# Kusto Sink Connector

Kusto Sink Connector allows writing data from a Flink DataStream to a table
in the specified Kusto cluster and database.

## Authentication

The connector uses **Azure Active Directory (AAD)** to authenticate the client application
that is using it. The authentication options are described in detail in [Authentication](Authentication.md). 

Please verify the following first:
* Client application is registered in AAD
* Client application has 'user' privileges or above on the target database
* When writing to an existing table, client application has 'ingestor' privileges while writing to the target. Note that 'admin' privileges on the target table (while using WriteAheadSink)

For details on Kusto principal roles, please refer to [Role-based Authorization](https://docs.microsoft.com/azure/kusto/management/access-control/role-based-authorization)
section in [Kusto Documentation](https://docs.microsoft.com/azure/kusto/).

For managing security roles, please refer to [Security Roles Management](https://docs.microsoft.com/azure/kusto/management/security-roles)
section in [Kusto Documentation](https://docs.microsoft.com/azure/kusto/).

## DataStream Sink

The Kusto sink connector allows writing data from a Flink DataStream to a table specified in the write options. There are 2 broad variants
- Sink connector extending [SinkV2 API](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/api/connector/sink2/SinkWriter.html).This provides a simple Kusto stateless sink that can flush 
data on checkpoint to achieve [at-least-once consistency](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/connector/base/DeliveryGuarantee.html) by default. 
Because there are no committers involved, this provides better performance than GenericWriteAheadSink (see below).This is the **_recommended option_** for large volume ingestion. 

- Sink connector extending [GenericWriteAheadSink API](https://nightlies.apache.org/flink/flink-docs-release-1.12/api/java/org/apache/flink/streaming/runtime/operators/GenericWriteAheadSink.html)
The Generic Sink implementation emits data to a KustoSink. This sink is integrated with Flink's checkpointing mechanism and can provide exactly-once guarantees. 
The sink uses a [Checkpoint committer](https://nightlies.apache.org/flink/flink-docs-release-1.12/api/java/org/apache/flink/streaming/runtime/operators/CheckpointCommitter.html) that is also implemented in Kusto 
Incoming records are stored within a AbstractStateBackend, and only committed if a checkpoint is completed.


#### Sink connector extending [SinkV2 API](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/api/connector/sink2/SinkWriter.html)
A simple usage example of using a V2 sink connector is shown below. The connector is initialized with the Kusto connection options and write options. This uses AAD application authentication.
The following configurations are used in the example below:

* **withDatabase**: The target database to write the data to.
* **withTable**: The target table to write the data to.
* **withBatchIntervalMs**: Ingestion to Kusto is optimized for few large batches as compared to large number of small batches. 
The connector buffers records in memory and flushes them to Kusto at the specified interval.Sets the batch flush interval, in milliseconds.
* **withBatchSize**: Sets the batch size to buffer records before flushing them to Kusto. The default is 1000 records
Note that BatchSize and BatchInterval are related. If both are passed the limit  that is reached first will trigger a flush.


```java
KustoConnectionOptions kustoConnectionOptions = KustoConnectionOptions.builder()
    .setAppId(appId).setAppKey(appKey).setTenantId(tenantId).setClusterUrl(cluster).build();

KustoWriteOptions kustoWriteOptions = KustoWriteOptions.builder()
    .withDatabase(<Database>).withTable(<Table name>).withBatchIntervalMs(30000)
    .withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

KustoWriteSink.builder().setWriteOptions(kustoWriteOptions)
.setConnectionOptions(kustoConnectionOptions).build(<Flink source datastream> /*Flink source data stream, example messages de-queued from Kafka*/, 2 /*Parallelism to use*/);

```
