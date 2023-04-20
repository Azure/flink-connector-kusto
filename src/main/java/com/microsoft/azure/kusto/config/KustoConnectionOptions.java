package com.microsoft.azure.kusto.config;

import java.io.Serializable;
import java.util.Objects;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The connection configuration class for Kusto. */
@PublicEvolving
public class KustoConnectionOptions implements Serializable {

    private final String clusterUrl;
    private final String database;
    private final String table;

    private final String appId;
    private final String appKey;
    private final String tenantId;
    private final boolean isManagedIdentity;
    private final String managedIdentityAppId;

    private KustoConnectionOptions(String clusterUrl, String database, String table, String appId, String appKey,
            String tenantId, boolean isManagedIdentity, String managedIdentityAppId) {
        this.clusterUrl = checkNotNull(clusterUrl);
        this.database = checkNotNull(database);
        this.table = checkNotNull(table);
        this.appId = appId;
        this.appKey = appKey;
        this.tenantId = tenantId;
        this.isManagedIdentity = isManagedIdentity;
        this.managedIdentityAppId = managedIdentityAppId;
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getAppId() {
        return appId;
    }

    public String getAppKey() {
        return appKey;
    }

    public String getTenantId() {
        return tenantId;
    }

    public boolean isManagedIdentity() {
        return isManagedIdentity;
    }

    public String getManagedIdentityAppId() {
        return managedIdentityAppId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KustoConnectionOptions that = (KustoConnectionOptions) o;
        return Objects.equals(clusterUrl, that.clusterUrl)
                && Objects.equals(database, that.database)
                && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterUrl, database, table);
    }

    public static KustoConnectionOptionsBuilder builder() {
        return new KustoConnectionOptionsBuilder();
    }

    /** Builder for {@link KustoConnectionOptions}. */
    @PublicEvolving
    public static class KustoConnectionOptionsBuilder {
        private String clusterUrl;
        private String database;
        private String table;

        private String appId;
        private String appKey;
        private String tenantId;
        private boolean isManagedIdentity;
        private String managedIdentityAppId;

        private KustoConnectionOptionsBuilder() {
        }

        /**
         * Sets the connection string of Kusto.
         *
         * @param clusterUrl connection string of Kusto
         * @return this builder
         */
        public KustoConnectionOptionsBuilder setClusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        /**
         * Sets the database of Kusto.
         *
         * @param database the database to sink of Kusto.
         * @return this builder
         */
        public KustoConnectionOptionsBuilder setDatabase(String database) {
            this.database = checkNotNull(database, "The database of Kusto must not be null");
            return this;
        }

        /**
         * Sets the table of Kusto.
         *
         * @param table the table to sink of Kusto.
         * @return this builder
         */
        public KustoConnectionOptionsBuilder setTable(String table) {
            this.table = checkNotNull(table, "The table of Kusto must not be null");
            return this;
        }

        public KustoConnectionOptionsBuilder setAppId(String appId) {
            this.isManagedIdentity = true;
            this.appId = checkNotNull(appId, "The appId for auth must not be null");
            return this;
        }

        public KustoConnectionOptionsBuilder setAppKey(String appKey) {
            this.appKey = checkNotNull(appKey, "The app key for auth must not be null");
            return this;
        }

        public KustoConnectionOptionsBuilder setTenantId(String tenantId) {
            this.tenantId = checkNotNull(tenantId, "The app key for auth must not be null");
            return this;
        }

        public KustoConnectionOptionsBuilder setManagedIdentityAppId(String managedIdentityAppId) {
            this.managedIdentityAppId = checkNotNull(managedIdentityAppId, "The User managed identity for auth must not be null");
            this.isManagedIdentity = true;
            return this;
        }

        /**
         * Build the {@link KustoConnectionOptions}.
         *
         * @return a KustoConnectionOptions with the settings made for this builder.
         */
        public KustoConnectionOptions build() {
            return new KustoConnectionOptions(clusterUrl, database, table, appId, appKey, tenantId, isManagedIdentity,
                    managedIdentityAppId);
        }
    }
}
