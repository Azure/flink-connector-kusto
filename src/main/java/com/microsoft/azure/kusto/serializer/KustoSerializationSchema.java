package com.microsoft.azure.kusto.serializer;

import java.io.Serializable;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

import com.microsoft.azure.kusto.config.KustoWriteOptions;
import com.microsoft.azure.kusto.context.KustoSinkContext;

@PublicEvolving
public interface KustoSerializationSchema<IN> extends Serializable {

    default void open(
            SerializationSchema.InitializationContext initializationContext,
            KustoSinkContext sinkContext,
            KustoWriteOptions sinkConfiguration)
            throws Exception {
        // Nothing to do by default.
    }

    KustoRow serialize(IN element,KustoSinkContext sinkContext);
}
