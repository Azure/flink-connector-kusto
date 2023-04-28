package com.microsoft.azure.flink.config;

import java.util.Objects;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KustoWriteOptions {
  private final String database;
  private final String table;

  private KustoWriteOptions(String database, String table) {
    this.database = checkNotNull(database);
    this.table = checkNotNull(table);
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
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

    public KustoWriteOptions build() {
      return new KustoWriteOptions(database, table);
    }
  }
}
