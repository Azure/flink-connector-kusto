package com.microsoft.azure.flink.writer.internal.committer;

import java.io.ByteArrayInputStream;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
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

@Internal
@PublicEvolving
public class KustoCommitter extends CheckpointCommitter {
  private static final long serialVersionUID = 1L;
  private final KustoConnectionOptions connectionOptions;
  private final KustoWriteOptions kustoWriteOptions;
  private IngestClient streamingIngestClient;
  private transient Client queryClient;
  private final String table = "flink_checkpoints";

  /**
   * A cache of the last committed checkpoint ids per subtask index. This is used to avoid redundant
   * round-trips to Kusto (see {@link #isCheckpointCommitted(int, long)}.
   */
  private final Map<Integer, Long> lastCommittedCheckpoints = new HashMap<>();

  /**
   * Creates a new {@link KustoCommitter} instance.
   *
   * @param connectionOptions The connection options for the Kusto cluster.
   * @param kustoWriteOptions The write options for the Kusto cluster.
   */
  public KustoCommitter(KustoConnectionOptions connectionOptions,
      KustoWriteOptions kustoWriteOptions) {
    this.connectionOptions = connectionOptions;
    this.kustoWriteOptions = kustoWriteOptions;
  }

  /** Internally used to set the job ID after instantiation. */
  public void setJobId(String id) throws Exception {
    super.setJobId(id);
  }

  /**
   * Generates the necessary tables to store information on the checkpoint.
   */
  @Override
  public void createResource() throws Exception {
    long startTime = Instant.now(Clock.systemUTC()).toEpochMilli();
    LOG.debug(
        "Creating resources for KustoCommitter. Creating table {} in database {} and applying policies",
        this.table, this.kustoWriteOptions.getDatabase());
    String createCheckpointTable = String.format(
        ".create-merge table %s (job_id:string, sink_id:string, sub_id:int, checkpoint_id:long) with (hidden=true,folder='Flink',docstring='Checkpointing table in Flink')",
        this.table);
    String enableStreaming = String.format(
        ".alter-merge table %s policy streamingingestion '{\"IsEnabled\": true}'", this.table);
    String retentionPolicy = String.format(
        ".alter-merge table %s policy retention softdelete = 1d recoverability = disabled", // TODO
                                                                                            // configurable
        this.table);
    try {
      queryClient.execute(this.kustoWriteOptions.getDatabase(), createCheckpointTable);
      queryClient.execute(this.kustoWriteOptions.getDatabase(), enableStreaming);
      queryClient.execute(this.kustoWriteOptions.getDatabase(), retentionPolicy);
    } catch (Exception e) {
      LOG.error(
          "Error while creating resources. To use the KustoCommitter you need to have admin privileges on the database {}",
          this.kustoWriteOptions.getDatabase(), e);
      throw e;
    }
    LOG.debug("Created resources for KustoCommitter. Table {} in database {} took {} ms",
        this.table, this.kustoWriteOptions.getDatabase(), Instant.now().toEpochMilli() - startTime);
  }

  /**
   * Creates the kusto streaming client needed for saving state for WAL.
   *
   * @throws Exception If an error occurs while creating the Kusto client
   */
  @Override
  public void open() throws Exception {
    LOG.debug("Opening KustoCommitter");
    if (this.connectionOptions == null || this.kustoWriteOptions == null) {
      throw new IllegalArgumentException(
          "No Connection options were provided or WriteOptions were null");
    }
    if (StringUtils.isEmpty(this.kustoWriteOptions.getDatabase())) {
      throw new IllegalArgumentException("Database provided was empty for KustoCommitter");
    }
    String thisClassName = KustoCommitter.class.getSimpleName();
    this.streamingIngestClient =
        KustoClientUtil.createMangedIngestClient(this.connectionOptions, thisClassName);
    // Need this to be available before the createResource call hits
    if (this.queryClient == null) {
      this.queryClient = KustoClientUtil.createClient(this.connectionOptions, thisClassName);
      LOG.info("Initialized queryClient in open and query client is null");
    }
    LOG.debug("Opened KustoCommitter");
  }

  @Override
  public void close() throws Exception {
    LOG.debug("Closing KustoCommitter");
    this.lastCommittedCheckpoints.clear();
    try {
      this.streamingIngestClient.close();
      this.queryClient.close();
    } catch (Exception e) {
      LOG.warn("Error while closing resources.", e);
      throw e;
    }
    LOG.debug("Closed KustoCommitter");
  }

  /**
   * Given a specific subtask and checkpoint id commits these values against the job to enable
   * recoverability.
   *
   * @param subtaskIdx The subtask index.
   * @param checkpointId The checkpoint id.
   */
  @Override
  public void commitCheckpoint(int subtaskIdx, long checkpointId) {
    long startTime = Instant.now().toEpochMilli();
    LOG.info("Starting checkpoint {} for subtask {} at {}", checkpointId, subtaskIdx,
        Instant.now().toEpochMilli());
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
    LOG.info("Committed checkpoint {} for subtask {} in {} ms", checkpointId, subtaskIdx,
        Instant.now().toEpochMilli() - startTime);
    lastCommittedCheckpoints.put(subtaskIdx, checkpointId);
  }

  /**
   * Checks if a checkpoint has been committed for a specific subtask.
   *
   * @param subtaskIdx The subtask index.
   * @param checkpointId The checkpoint id.
   * @return True if the checkpoint has been committed, false otherwise.
   */
  @Override
  public boolean isCheckpointCommitted(int subtaskIdx, long checkpointId) {
    long startTime = Instant.now().toEpochMilli();
    Long lastCommittedCheckpoint = lastCommittedCheckpoints.get(subtaskIdx);
    if (lastCommittedCheckpoint == null) {
      String statement = String.format(
          "%s | where job_id == '%s' and sink_id == '%s' and sub_id == %d and checkpoint_id >= %d | top 1 by checkpoint_id desc",
          this.table, this.jobId, this.operatorId, subtaskIdx, checkpointId);
      try {
        KustoOperationResult checkpoints =
            queryClient.execute(this.kustoWriteOptions.getDatabase(), statement);
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
    LOG.info("Checkpoint query took {} ms", Instant.now().toEpochMilli() - startTime);
    return lastCommittedCheckpoint != null && checkpointId <= lastCommittedCheckpoint;
  }
}
