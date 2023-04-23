package com.microsoft.azure.flink.writer.serializer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

public class KustoSinkElementConverter<IN> implements ElementConverter<IN, KustoRow> {

    private boolean schemaOpened = false;

    /** A serialization schema to specify how the input element should be serialized. */
    private final SerializationSchema<IN> serializationSchema;

    private KustoSinkElementConverter(SerializationSchema<IN> serializationSchema) {
        this.serializationSchema = serializationSchema;
    }

    @Override
    public KustoRow apply(IN element, SinkWriter.Context context) {
        checkOpened();
        return new KustoRow("",new Object[]{});
    }

    private void checkOpened() {
        if (!schemaOpened) {
            try {
                serializationSchema.open(
                        new SerializationSchema.InitializationContext() {
                            @Override
                            public MetricGroup getMetricGroup() {
                                return new UnregisteredMetricsGroup();
                            }

                            @Override
                            public UserCodeClassLoader getUserCodeClassLoader() {
                                return SimpleUserCodeClassLoader.create(
                                        KustoSinkElementConverter.class.getClassLoader());
                            }
                        });
                schemaOpened = true;
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to initialize serialization schema.", e);
            }
        }
    }

    public static <IN> Builder<IN> builder() {
        return new Builder<>();
    }

    /** A builder for the KinesisFirehoseSinkElementConverter. */
    public static class Builder<IN> {

        private SerializationSchema<IN> serializationSchema;

        public Builder<IN> setSerializationSchema(
                SerializationSchema<IN> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return this;
        }

        public KustoSinkElementConverter<IN> build() {
            Preconditions.checkNotNull(
                    serializationSchema,
                    "No SerializationSchema was supplied to the KustoSinkElementConverter builder.");
            return new KustoSinkElementConverter<>(serializationSchema);
        }
    }
}

