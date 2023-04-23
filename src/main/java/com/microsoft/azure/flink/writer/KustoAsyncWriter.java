//package com.microsoft.azure.flink.writer;
//
//import java.util.Collection;
//import java.util.List;
//import java.util.function.Consumer;
//
//import org.apache.flink.api.connector.sink2.Sink;
//import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
//import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
//import org.apache.flink.connector.base.sink.writer.ElementConverter;
//import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
//import org.apache.flink.metrics.Counter;
//import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.microsoft.azure.flink.writer.serializer.KustoRow;
//import com.microsoft.azure.kusto.ingest.IngestClient;
//
//public class KustoAsyncWriter<IN> extends AsyncSinkWriter<IN, KustoRow> {
//
//  private static final Logger LOG = LoggerFactory.getLogger(KustoAsyncWriter.class);
//
//
//  private final Counter numRecordsOutErrorsCounter;
//
//  /* Name of the table to upload data to */
//  private final String tableName;
//
//  /* The sink writer metric group */
//  private final SinkWriterMetricGroup metrics;
//
//  /* The Kusto client */
//  private final IngestClient kustoIngestClient;
//
//  /* Flag to whether fatally fail any time we encounter an exception when persisting records */
//  private final boolean failOnError;
//
//
//
//  public KustoAsyncWriter(ElementConverter<IN, KustoRow> elementConverter, Sink.InitContext context,
//                          AsyncSinkWriterConfiguration configuration,
//                          Collection<BufferedRequestState<KustoRow>> bufferedRequestStates,
//                          Counter numRecordsOutErrorsCounter, String tableName, SinkWriterMetricGroup metrics,
//                          IngestClient kustoIngestClient, boolean failOnError) {
//    super(elementConverter, context, configuration, bufferedRequestStates);
//
//  }
//
//  @Override
//  protected void submitRequestEntries(List<KustoRow> list, Consumer<List<KustoRow>> consumer) {
//
//    /*
//    * Here are the list of steps to do:
//    a) Get containers for blobs that can be written into
//    b) Take each KustoRow and write it into the GZIP blob
//    c) Once the blob is full, use the kustoClient and intimate that a new blob is ready for ingestion
//    d) Once ingested poll for the status of ingestion and if it fails, retry. May be we should have a ingestByTags for non duplicates
//    e) Upload the next blobs
//    *
//    * */
//  }
//
//  @Override
//  protected long getSizeInBytes(KustoRow kustoRow) {
//    return 0;
//  }
//}
