package com.microsoft.azure.kusto.writer;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.writer.serializer.KustoRow;

public class KustoAsyncWriter<IN> extends AsyncSinkWriter<IN, KustoRow> {

  private static final Logger LOG = LoggerFactory.getLogger(KustoAsyncWriter.class);


  private final Counter numRecordsOutErrorsCounter;

  /* Name of the delivery stream in Kinesis Data Firehose */
  private final String deliveryStreamName;

  /* The sink writer metric group */
  private final SinkWriterMetricGroup metrics;

  /* The asynchronous Firehose client */
  private final IngestClient kustoIngestClient;

  /* Flag to whether fatally fail any time we encounter an exception when persisting records */
  private final boolean failOnError;



  public KustoAsyncWriter(ElementConverter<IN, KustoRow> elementConverter, Sink.InitContext context,
      AsyncSinkWriterConfiguration configuration,
      Collection<BufferedRequestState<KustoRow>> bufferedRequestStates,
      Counter numRecordsOutErrorsCounter, String deliveryStreamName, SinkWriterMetricGroup metrics,
      IngestClient kustoIngestClient, boolean failOnError) {
    super(elementConverter, context, configuration, bufferedRequestStates);
    this.numRecordsOutErrorsCounter = numRecordsOutErrorsCounter;
    this.deliveryStreamName = deliveryStreamName;
    this.metrics = metrics;
    this.kustoIngestClient = kustoIngestClient;
    this.failOnError = failOnError;
  }

  @Override
  protected void submitRequestEntries(List<KustoRow> list, Consumer<List<KustoRow>> consumer) {

  }

  @Override
  protected long getSizeInBytes(KustoRow kustoRow) {
    return 0;
  }
}
