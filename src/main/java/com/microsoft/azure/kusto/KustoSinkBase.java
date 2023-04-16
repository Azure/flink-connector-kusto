package com.microsoft.azure.kusto;

import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.config.KustoConnectionOptions;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;

public abstract class KustoSinkBase<IN,V> extends RichSinkFunction<IN>  implements CheckpointedFunction {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final KustoConnectionOptions options;
    private final KustoSink.KustoSinkBuilder builder;

    private AtomicReference<Throwable> throwable;
    private CompletableFuture<V> callback;

    private transient IngestClient ingestClient;

    KustoSinkBase(KustoConnectionOptions options, KustoSink.KustoSinkBuilder builder) {
        this.options = options;
        this.builder = builder;
    }

    public void open(Configuration configuration) {
        try {

            ConnectionStringBuilder kcsb = options.isManagedIdentity() ?
                    ConnectionStringBuilder.createWithAadManagedIdentity(options.getClusterUrl(), options.getManagedIdentityAppId())
                    : ConnectionStringBuilder.createWithAadApplicationCredentials(options.getClusterUrl(), options.getAppId(), options.getAppKey(), options.getTenantId());
            ingestClient = IngestClientFactory.createClient(kcsb);
            this.callback = new CompletableFuture<V>().whenComplete((v, t) -> {
                if (t != null) {
                    throwable.set(t);
                } else {
                    log.info("Ingestion completed");
                }
            });
        } catch (URISyntaxException e) {
            log.error("Failed to create ingest client", e);
            throw new RuntimeException(e);
        }
    }
}