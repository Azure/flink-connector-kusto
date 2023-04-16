package com.microsoft.azure.kusto.context;

import org.apache.flink.api.connector.sink2.Sink;

import com.microsoft.azure.kusto.config.KustoWriteOptions;

public class DefaultKustoSinkContext implements KustoSinkContext{

    private final Sink.InitContext initContext;
    private final KustoWriteOptions writeOptions;

    public DefaultKustoSinkContext(Sink.InitContext initContext, KustoWriteOptions writeOptions) {
        this.initContext = initContext;
        this.writeOptions = writeOptions;
    }

    @Override
    public Sink.InitContext getInitContext() {
        return this.initContext;
    }

    @Override
    public long processTime() {
        return initContext.getProcessingTimeService().getCurrentProcessingTime();
    }

    @Override
    public KustoWriteOptions getWriteOptions() {
        return this.writeOptions;
    }
}
