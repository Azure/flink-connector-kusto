package com.microsoft.azure.flink.config;

import java.util.Objects;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KustoWriteOptions {
  private final String database;
  private final String table;

  private final String ingestionMappingRef;

  private final boolean flushImmediately;

  private KustoWriteOptions(String database, String table, String ingestionMappingRef,
      boolean flushImmediately) {
    this.database = checkNotNull(database);
    this.table = checkNotNull(table);
    this.ingestionMappingRef = ingestionMappingRef;
    this.flushImmediately = flushImmediately;
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



  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KustoWriteOptions that = (KustoWriteOptions) o;
    return Objects.equals(database, that.database) && Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, table);
  }

  public static KustoWriteOptions.Builder builder() {
    return new KustoWriteOptions.Builder();
  }

  /** Builder for {@link KustoWriteOptions}. */
  @PublicEvolving
  public static class Builder {
    private String database;
    private String table;

    private String ingestionMappingRef = null;

    private boolean flushImmediately = false;

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


    public KustoWriteOptions build() {
      return new KustoWriteOptions(database, table, ingestionMappingRef, flushImmediately);
    }
  }
}
