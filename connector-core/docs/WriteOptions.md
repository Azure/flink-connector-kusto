# Kusto Sink Connector

ADX/Kusto Sink Connector allows writing data from a Flink DataStream to a table
in the specified Kusto cluster and database.

This sink connector can write Flink datastream to the following ADX/Kusto cluster types.
[Azure Data Explorer](https://docs.microsoft.com/en-us/azure/data-explorer),
[Azure Synapse Data Explorer](https://docs.microsoft.com/en-us/azure/synapse-analytics/data-explorer/data-explorer-overview) and
[Real time analytics in Fabric](https://learn.microsoft.com/en-us/fabric/real-time-analytics/overview)

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

## Basic Configurations

The connector uses the KustoConnectionOptions and KustoWriteOptions to configure the connection to the ADX/Kusto cluster and the write options respectively.

**KustoConnectionOptions** are used to configure the connection to the ADX/Kusto cluster. 
- **Authentication**: Refer authentication options specified in [Authentication](Authentication.md).
- **RetryOptions**: Retry options for the ADX/Kusto client. While performing ADX/Kusto ingestion, the following retry options can be specified.
  - **withMaxAttempts**: Maximum number of attempts to retry ingestion. Default is 3.
  - **withBaseIntervalMillis**: Base interval in milliseconds between retries. Default is 1000.
  - **withMaxIntervalMillis**: Maximum interval in milliseconds between retries. Default is 60000.
  - **withCacheExpirationSeconds**: Cache expiration in seconds for the ADX/Kusto container resources. For ingestion ADX/Kusto ingestion containers are used. 
  The query to get these ingestion resources is cached. Default is 120 * 60 (2 hours).

```java
KustoRetryConfig kustoRetryConfig = KustoRetryConfig.builder()
        .withBaseIntervalMillis(2000L).withMaxIntervalMillis(120000L).withCacheExpirationSeconds(120*60).withMaxAttempts(cluster).build();
```
- **ClusterUrl**: The cluster url to connect to. This is of the form https://<cluster-name>.<region>.kusto.windows.net (or) https://<fabric-cluster>.kusto.fabric.microsoft.com
```java
KustoConnectionOptions kustoConnectionOptions = KustoConnectionOptions.builder().setClusterUrl(cluster).build();
```

**KustoWriteOptions** are used to configure the various write options to the ADX cluster.
- **Database**: The target database to write the data to.
- **Table**: The target table to write the data to.
- **IngestionMappingRef**: Ingestion mapping reference. References an existing [mapping](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/management/create-ingestion-mapping-command) defined on Kusto
- **FlushImmediately**: **Not Recommended**. Flushes the data to ADX/Kusto [immediately](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/netfx/kusto-ingest-client-reference#class-kustoqueuedingestionproperties) without aggregation. This is not recommended as it can cause performance issues.
- **BatchIntervalMs**: Ingestion to Kusto is optimized for few large batches as compared to large number of small batches.The interval controls how often is the data flushed to ADX/Kusto. The default is 30 seconds.
- **BatchSize**: Sets the batch size to buffer records before flushing them to ADX/Kusto. The default is 1000 records
- **ClientBatchSizeLimit**: When the batch interval and batch size are specified, the limit indicating the size in MB of the 
aggregated data before ingested to ADX/Kusto. If 1.25GB of data is written in a 30 second batch interval, and 500MB is specified as the batch limit, 
there will be 3 blobs of 500,500,250 MB sized blobs will be ingested.The default is 300 MB.
- **PollForIngestionStatus**: If set to true, the connector will poll for ingestion status after flushing the data to ADX/Kusto.The default is false
- **DeliveryGuarantee**: References [DeliveryGuarantee](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/connector/base/DeliveryGuarantee.html) semantics in flink.
Defaults to DeliveryGuarantee.AT_LEAST_ONCE. Note that though EXACTLY_ONCE semantics are specified, there are failure/retry scenarios where there can be duplicates in the Sink. A more bulletproof 
way to achieve exactly-once semantics is to use the GenericWriteAheadSink (see below).

```java
KustoWriteOptions kustoWriteOptions = KustoWriteOptions.builder()
        .withDatabase( database).withTable( tableName).withBatchIntervalMs(30000)
        .withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
```

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

- **withDatabase**: The target database to write the data to.
- **withTable**: The target table to write the data to.
- **withBatchIntervalMs**: Ingestion to Kusto is optimized for few large batches as compared to large number of small batches. 
The connector buffers records in memory and flushes them to Kusto at the specified interval.Sets the batch flush interval, in milliseconds.
- **withBatchSize**: Sets the batch size to buffer records before flushing them to Kusto. The default is 1000 records
Note that BatchSize and BatchInterval are related. If both are passed the limit  that is reached first will trigger a flush.


```java
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;

String database = "database-change-me";
String tableName = "table-name-change-me";
KustoConnectionOptions kustoConnectionOptions = KustoConnectionOptions.builder()
        .setAppId(appId).setAppKey(appKey).setTenantId(tenantId).setClusterUrl(cluster).build();

KustoWriteOptions kustoWriteOptions = KustoWriteOptions.builder()
        .withDatabase( database).withTable( tableName).withBatchIntervalMs(30000)
        .withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

KustoWriteSink.builder().setWriteOptions(kustoWriteOptions)
        .setConnectionOptions(kustoConnectionOptions).build( <Flink source datastream> /*Flink source data stream, example messages de-queued from Kafka*/
        ,2 /*Parallelism to use*/);
```

#### Sink connector extending [GenericWriteAheadSink API](https://nightlies.apache.org/flink/flink-docs-release-1.12/api/java/org/apache/flink/streaming/runtime/operators/GenericWriteAheadSink.html)
A simple usage example of using a generic sink connector is shown below. The connector is initialized with the Kusto connection options and write options. This uses AAD application authentication.
The following configurations are used in the example below:

- **withDatabase**: The target database to write the data to.
- **withTable**: The target table to write the data to.

This sink is integrated with Flink's checkpointing mechanism and can provide exactly-once guarantees; depending on the storage backend and sink/committer implementation.
The sink uses a [Checkpoint committer](https://nightlies.apache.org/flink/flink-docs-release-1.12/api/java/org/apache/flink/streaming/runtime/operators/CheckpointCommitter.html) that is also implemented in Kusto. 
Following the semantics of the Sink, only if the checkpoint is completed, the records are committed to Kusto. Batch size and Batch interval are not pertinent for this sink.

Note that admin privileges are required for this sink type to work. The committer internally creates tables to save the state of the checkpoint so that EXACTLY_ONCE semantics can be achieved. Because of this overhead
this is typically **slower** than the V2 sink connector.

```java
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;

String database = "database-change-me";
String tableName = "table-name-change-me";
KustoConnectionOptions kustoConnectionOptions = KustoConnectionOptions.builder()
        .setAppId(appId).setAppKey(appKey).setTenantId(tenantId).setClusterUrl(cluster).build();

KustoWriteOptions kustoWriteOptions = KustoWriteOptions.builder()
        .withDatabase( database).withTable( tableName).build();

KustoWriteSink.builder().setWriteOptions(kustoWriteOptions)
        .setConnectionOptions(kustoConnectionOptions).buildWriteAheadSink( <Flink source datastream> /*Flink source data stream, example messages de-queued from Kafka*/
        ,2 /*Parallelism to use*/);
```
## Samples

A simple java implementation is provided in the [samples-java](../../samples-java) folder. The sample uses a simple websocket connector to [track crypto prices and trades](https://docs.cloud.coinbase.com/exchange/docs/websocket-overview)
and sink the data to ADX/Kusto.

A simple k8s cluster for tests can be set up using the sample k8s manifest provided in [resources](../../samples-java/src/main/resources/deployment) folder. The manifest creates a k8s cluster with a job-manager and task-manager.
The samples can be packaged using ```mvn clean package``` and the jar can be copied to the job-manager pod using ```kubectl cp```. The sample can be run using ```kubectl exec -it <job-manager-pod> -- /opt/flink/bin/flink run -c com.microsoft.azure.kusto.samples.KustoSinkSample <jar-name>```. 
The deployment of the jar can be performed from job-manager UI as well by uploading the jar file. 


## Troubleshooting
The task-manager logs usually provides the best information on the errors encountered. The logs can be accessed using ```kubectl logs <task-manager-pod>```. 
Changing log level in Flink will emit additional logs this will usually provide a good indication of the error.