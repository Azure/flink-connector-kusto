package com.microsoft.azure.flink.config;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Options for configuring a Kusto sink. Passes on ClientOptions. Also provides a set of developer
 * options to fine tune behavior
 */
public class KustoWriteOptions implements Serializable {
  protected static final Logger LOG = LoggerFactory.getLogger(KustoWriteOptions.class);
  /* The database to write the data to */
  private final String database;
  /* The ingestion happens to this table */
  private final String table;
  /* The ingestion mapping reference to use */
  private final String ingestionMappingRef;
  /* If set to true, any aggregation will be skipped. Avoid using this unless really necessary. */
  private final boolean flushImmediately;
  /*
   * Applicable in non WAL sink only. In case of simple sink this determines how often records are
   * collected and pushed for ingestion
   */
  private final long batchIntervalMs;
  /*
   * Applicable in non WAL sink only. In case of simple sink this determines the size of the
   * collected records & pushed for ingestion
   */
  private final long batchSize;
  private final long clientBatchSizeLimit;
  private final List<String> ingestByTags;
  private final List<String> additionalTags;
  private final DeliveryGuarantee deliveryGuarantee;
  private final boolean pollForIngestionStatus;

  private KustoWriteOptions(@NotNull Builder builder) {
    this.database = checkNotNull(builder.database);
    this.table = checkNotNull(builder.table);
    this.ingestionMappingRef = builder.ingestionMappingRef;
    this.flushImmediately = builder.flushImmediately;
    if (flushImmediately) {
      LOG.warn("FlushImmediately is set to true, this may cause performance issues");
    }
    this.batchIntervalMs = builder.batchIntervalMs;
    this.batchSize = builder.batchSize;
    this.clientBatchSizeLimit = builder.clientBatchSizeLimit;
    this.ingestByTags = builder.ingestByTags;
    this.additionalTags = builder.additionalTags;
    this.deliveryGuarantee = builder.deliveryGuarantee;
    this.pollForIngestionStatus = builder.pollForIngestionStatus;
  }

  public List<String> getIngestByTags() {
    return ingestByTags;
  }

  public List<String> getAdditionalTags() {
    return additionalTags;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public String getIngestionMappingRef() {
    return ingestionMappingRef;
  }

  public boolean getFlushImmediately() {
    return flushImmediately;
  }

  public long getBatchIntervalMs() {
    return batchIntervalMs;
  }

  public long getBatchSize() {
    return batchSize;
  }

  public long getClientBatchSizeLimit() {
    return clientBatchSizeLimit;
  }

  public DeliveryGuarantee getDeliveryGuarantee() {
    return deliveryGuarantee;
  }

  public boolean getPollForIngestionStatus() {
    return pollForIngestionStatus;
  }


  @Override
  public int hashCode() {
    return Objects.hash(database, table);
  }

  @Contract(" -> new")
  public static KustoWriteOptions.@NotNull Builder builder() {
    return new KustoWriteOptions.Builder();
  }

  @Override
  public String toString() {
    return "KustoWriteOptions{" + "database='" + database + '\'' + ", table='" + table + '\''
        + ", ingestionMappingRef='" + ingestionMappingRef + '\'' + ", flushImmediately="
        + flushImmediately + ", batchIntervalMs=" + batchIntervalMs + ", batchSize=" + batchSize
        + ", clientBatchSizeLimit=" + clientBatchSizeLimit + ", ingestByTags=" + ingestByTags
        + ", additionalTags=" + additionalTags + ", deliveryGuarantee=" + deliveryGuarantee
        + ", pollForIngestionStatus=" + pollForIngestionStatus + '}';
  }

  /** Builder for {@link KustoWriteOptions}. */
  @PublicEvolving
  public static class Builder {
    private String database;
    private String table;
    private String ingestionMappingRef = null;
    private boolean flushImmediately = false;
    private long batchIntervalMs = 30000L; // 30 seconds
    private long batchSize = 1000L; // Or 1000 records
    private long clientBatchSizeLimit = 300 * 1024 * 1024; // Or 300 MB
    private boolean pollForIngestionStatus = false; // Or 300 MB
    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;

    private List<String> ingestByTags = Collections.emptyList();

    private List<String> additionalTags = Collections.emptyList();

    private Builder() {}

    /**
     * Sets the database .
     *
     * @param database the database to sink .
     * @return this builder
     */
    public KustoWriteOptions.Builder withDatabase(String database) {
      this.database = checkNotNull(database, "The database  must not be null");
      return this;
    }

    /**
     * Sets the table .
     *
     * @param table the table to sink .
     * @return this builder
     */
    public KustoWriteOptions.Builder withTable(String table) {
      this.table = checkNotNull(table, "The table  must not be null");
      return this;
    }

    /**
     * Sets the ingestion mapping reference .
     *
     * @param ingestionMappingRef the ingestion mapping reference to sink .
     * @return this builder
     */
    public KustoWriteOptions.Builder withIngestionMappingRef(String ingestionMappingRef) {
      this.ingestionMappingRef =
          checkNotNull(ingestionMappingRef, "The mapping ingestion reference  must not be null");
      return this;
    }

    /**
     * Sets the flush immediately .
     *
     * @param flushImmediately the flush immediately to sink .
     * @return this builder
     */
    public KustoWriteOptions.Builder withFlushImmediately(boolean flushImmediately) {
      this.flushImmediately = flushImmediately;
      return this;
    }

    /**
     * Sets the number of records to sink in a single batch
     *
     * @param batchSize the batch size of records to sink in a single batch
     * @return this builder
     */
    public KustoWriteOptions.Builder withBatchSize(long batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Sets the batch interval in milliseconds
     *
     * @param batchIntervalMs the batch interval in milliseconds
     * @return this builder
     */

    public KustoWriteOptions.Builder withBatchIntervalMs(long batchIntervalMs) {
      this.batchIntervalMs = batchIntervalMs;
      return this;
    }

    /**
     * Sets the ingest by tags
     *
     * @param ingestByTags the ingest by tags
     * @return this builder
     */
    public KustoWriteOptions.Builder withIngestByTags(List<String> ingestByTags) {
      this.ingestByTags = ingestByTags;
      return this;
    }

    /**
     * Sets the DeiveryGuarantee
     * 
     * @param deliveryGuarantee The delivery guarantee, this can be None (fastest), AtLeastOnce
     *        (default) or ExactlyOnce (slowest)
     * @return this builder
     */
    public KustoWriteOptions.Builder withDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
      if (deliveryGuarantee != null) {
        if (deliveryGuarantee == DeliveryGuarantee.NONE) {
          LOG.info(DeliveryGuarantee.NONE.getDescription().toString());
        }
        this.deliveryGuarantee = deliveryGuarantee;
      }
      return this;
    }

    /**
     * Sets the Client batch size limit. The ingest to Kusto happens through a blob ingest. The
     * ingestion is optimized for large files (1GB compressed,4 GB deflated). This parameter
     * determines the size of the file that is created before it is ingested to Kusto.If the stream
     * is slow, it may take longer to reach this limit. Also refer
     * 
     * @see #withBatchSize for the number of records that are collected before the file is created.
     * @see #withBatchIntervalMs for the batch interval in milliseconds
     * @param clientBatchSizeLimit The client batch size limit
     * @return this builder
     */
    public KustoWriteOptions.Builder withClientBatchSizeLimit(long clientBatchSizeLimit) {
      this.clientBatchSizeLimit = clientBatchSizeLimit;
      return this;
    }

    public KustoWriteOptions.Builder withPollForIngestionStatus(boolean pollForIngestionStatus) {
      this.pollForIngestionStatus = pollForIngestionStatus;
      return this;
    }

    /**
     * Builds a {@link KustoWriteOptions} instance.
     * 
     * @return a {@link KustoWriteOptions} instance
     */
    public KustoWriteOptions build() {
      if (batchIntervalMs > 0 || batchSize > 0) {
        LOG.warn(
            "BatchInterval and BatchSize are applicable options only for SinkWriter and not applicable for GenericWriteAheadSink");
      }
      return new KustoWriteOptions(this);
    }
  }
}
