package com.microsoft.azure.flink.writer.internal.committer;

import java.io.ByteArrayInputStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;

import com.microsoft.azure.flink.common.KustoClientUtil;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

public class KustoCommitter extends CheckpointCommitter {

  private static final long serialVersionUID = 1L;

  private final KustoConnectionOptions connectionOptions;
  private final KustoWriteOptions kustoWriteOptions;
  private IngestClient streamingIngestClient;
  private final Client queryClient;

  private final String table = "flink_checkpoints";



  /**
   * A cache of the last committed checkpoint ids per subtask index. This is used to avoid redundant
   * round-trips to Cassandra (see {@link #isCheckpointCommitted(int, long)}.
   */
  private final Map<Integer, Long> lastCommittedCheckpoints = new HashMap<>();

  public KustoCommitter(KustoConnectionOptions connectionOptions,
      KustoWriteOptions kustoWriteOptions) throws URISyntaxException {
    this.connectionOptions = connectionOptions;
    this.kustoWriteOptions = kustoWriteOptions;
    this.queryClient = KustoClientUtil.createClient(this.connectionOptions);
  }


  /** Internally used to set the job ID after instantiation. */
  public void setJobId(String id) throws Exception {
    super.setJobId(id);
  }

  /**
   * Generates the necessary tables to store information.
   *
   */
  @Override
  public void createResource() throws Exception {
    String createCheckpointTable = String.format(
        ".create-merge table %s (job_id:string, sink_id:string, sub_id:int, checkpoint_id:long) with (hidden=true,folder='Flink',docstring='Checkpointing table in Flink')",
        this.table);
    String retentionPolicy = String.format(
        ".alter-merge table %s policy retention softdelete = 1d recoverability = disabled",//TODO make this configurable
        this.table);
    try {
      queryClient.execute(this.kustoWriteOptions.getDatabase(), createCheckpointTable);
      queryClient.execute(this.kustoWriteOptions.getDatabase(), retentionPolicy);
    } catch (Exception e) {
      LOG.error(
          "Error while creating resources. To use the KustoCommitter you need to have admin privileges on the database {}",
          this.kustoWriteOptions.getDatabase(), e);
      throw e;
    }
  }

  @Override
  public void open() throws Exception {
    if (this.connectionOptions == null || this.kustoWriteOptions == null) {
      throw new RuntimeException("No Connection options were provided or WriteOptions were null");
    }
    if (StringUtils.isEmpty(this.kustoWriteOptions.getDatabase())) {
      throw new RuntimeException("No Connection options were provided or WriteOptions were null");
    }
    this.streamingIngestClient = KustoClientUtil.createMangedIngestClient(this.connectionOptions);
  }

  @Override
  public void close() throws Exception {
    this.lastCommittedCheckpoints.clear();
    try {
      this.streamingIngestClient.close();
      this.queryClient.close();
    } catch (Exception e) {
      LOG.warn("Error while closing resources.", e);
    }
  }

  @Override
  public void commitCheckpoint(int subtaskIdx, long checkpointId) {
    String payload = String.format("%s,%s,%d,%d", StringEscapeUtils.escapeCsv(jobId),
        StringEscapeUtils.escapeCsv(operatorId), subtaskIdx, checkpointId);
    IngestionProperties properties =
        new IngestionProperties(this.kustoWriteOptions.getDatabase(), this.table);
    properties.setFlushImmediately(true);
    properties.setDataFormat(IngestionProperties.DataFormat.CSV);
    try {
      this.streamingIngestClient.ingestFromStream(
          new StreamSourceInfo(new ByteArrayInputStream(payload.getBytes())), properties);
    } catch (IngestionClientException | IngestionServiceException e) {
      LOG.warn("Error performing checkpoint ingestion on table {} in database {}.", this.table,
          this.kustoWriteOptions.getDatabase(), e);
      throw new RuntimeException(e);
    }
    lastCommittedCheckpoints.put(subtaskIdx, checkpointId);
  }

  @Override
  public boolean isCheckpointCommitted(int subtaskIdx, long checkpointId) {
    Long lastCommittedCheckpoint = lastCommittedCheckpoints.get(subtaskIdx);
    if (lastCommittedCheckpoint == null) {
      String statement = String.format(
          "%s | where job_id == '%s' and sink_id == '%s' and sub_id == %d and checkpoint_id >= %d | top 1 by checkpoint_id desc",
          this.table, this.jobId, this.operatorId, subtaskIdx, checkpointId);
      try {
        KustoOperationResult checkpoints =
            this.queryClient.execute(this.kustoWriteOptions.getDatabase(), statement);
        if (checkpoints != null && checkpoints.getPrimaryResults() != null
            && checkpoints.getPrimaryResults().getData().size() > 0) {
          lastCommittedCheckpoint =
              Long.parseLong(checkpoints.getPrimaryResults().getData().get(0).toString());
          lastCommittedCheckpoints.put(subtaskIdx, lastCommittedCheckpoint);
          return true;
        }
      } catch (DataServiceException | DataClientException e) {
        throw new RuntimeException(e);
      }
    }
    return lastCommittedCheckpoint != null && checkpointId <= lastCommittedCheckpoint;
  }
}