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

  private KustoWriteOptions(String database, String table, String ingestionMappingRef,
      boolean flushImmediately, long batchIntervalMs, long batchSize, long clientBatchSizeLimit,
      List<String> ingestByTags, List<String> additionalTags, DeliveryGuarantee deliveryGuarantee,
      boolean pollForIngestionStatus) {
    this.database = checkNotNull(database);
    this.table = checkNotNull(table);
    this.ingestionMappingRef = ingestionMappingRef;
    this.flushImmediately = flushImmediately;
    if (flushImmediately) {
      LOG.warn("FlushImmediately is set to true, this may cause performance issues");
    }
    this.batchIntervalMs = batchIntervalMs;
    this.batchSize = batchSize;
    this.clientBatchSizeLimit = clientBatchSizeLimit;
    this.ingestByTags = ingestByTags;
    this.additionalTags = additionalTags;
    this.deliveryGuarantee = deliveryGuarantee;
    this.pollForIngestionStatus = pollForIngestionStatus;
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
    private long batchIntervalMs = -1L; // Not applicable by default
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
     * Sets the additional tags
     *
     * @param additionalTags the additional tags
     * @return this builder
     */

    public KustoWriteOptions.Builder withAdditionalTags(List<String> additionalTags) {
      this.additionalTags = additionalTags;
      return this;
    }

    public KustoWriteOptions.Builder withDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
      if(deliveryGuarantee != null) {
        if(deliveryGuarantee == DeliveryGuarantee.NONE){
          LOG.info(DeliveryGuarantee.NONE.getDescription().toString());
        }
        this.deliveryGuarantee = deliveryGuarantee;
      }
      return this;
    }

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
      return new KustoWriteOptions(database, table, ingestionMappingRef, flushImmediately,
          batchIntervalMs, batchSize, clientBatchSizeLimit, ingestByTags, additionalTags,
          deliveryGuarantee, pollForIngestionStatus);
    }
  }
}
