package com.microsoft.azure.kusto.writer.context;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;

import com.microsoft.azure.kusto.config.KustoWriteOptions;

@PublicEvolving
public interface KustoSinkContext {

    /** Returns the current sink's init context. */
    Sink.InitContext getInitContext();

    /** Returns the current process time in flink. */
    long processTime();

    /** Returns the write options of KustoSink. */
    KustoWriteOptions getWriteOptions();
}
