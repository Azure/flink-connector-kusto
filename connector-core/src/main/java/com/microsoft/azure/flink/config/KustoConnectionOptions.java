package com.microsoft.azure.flink.config;

import java.io.Serializable;
import java.util.Objects;

import org.apache.flink.annotation.PublicEvolving;
import org.jetbrains.annotations.NotNull;

import com.microsoft.azure.flink.common.KustoRetryConfig;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The connection configuration class for Kusto. */
@PublicEvolving
public class KustoConnectionOptions implements Serializable {
  private static final String INGEST_PREFIX = "ingest-";
  private static final String PROTOCOL_SUFFIX = "://";
  private final String clusterUrl;
  private final String ingestUrl;
  private final String appId;
  private final String appKey;
  private final String tenantId;
  private final boolean isManagedIdentity;
  private final String managedIdentityAppId;
  private final KustoRetryConfig kustoRetryConfig;

  private KustoConnectionOptions(@NotNull Builder builder) {
    this.clusterUrl = checkNotNull(builder.clusterUrl);
    this.ingestUrl = clusterUrl.replaceFirst(PROTOCOL_SUFFIX, PROTOCOL_SUFFIX + INGEST_PREFIX);
    this.appId = builder.appId;
    this.appKey = builder.appKey;
    this.tenantId = builder.tenantId;
    this.isManagedIdentity = builder.isManagedIdentity;
    this.managedIdentityAppId = builder.managedIdentityAppId;
    this.kustoRetryConfig = builder.kustoRetryConfig;
  }

  public String getClusterUrl() {
    return clusterUrl;
  }

  public String getIngestUrl() {
    return ingestUrl;
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

  public KustoRetryConfig getKustoRetryConfig() {
    return kustoRetryConfig;
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
    return Objects.equals(clusterUrl, that.clusterUrl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterUrl);
  }

  @Override
  public String toString() {
    return "KustoConnectionOptions{" + "clusterUrl='" + clusterUrl + '\'' + ", ingestUrl='"
        + ingestUrl + '\'' + ", appId='" + appId + '\'' + ", isManagedIdentity='"
        + isManagedIdentity + '\'' + ", managedIdentityAppId='" + managedIdentityAppId + '\'' + '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link KustoConnectionOptions}. */
  @PublicEvolving
  public static class Builder {
    private String clusterUrl;
    private String appId;
    private String appKey;
    private String tenantId;
    private boolean isManagedIdentity = false;
    private String managedIdentityAppId;
    private KustoRetryConfig kustoRetryConfig = new KustoRetryConfig.Builder().build();

    private Builder() {}

    /**
     * Sets the connection string of Kusto.
     *
     * @param clusterUrl connection string of Kusto
     * @return this builder
     */
    public Builder setClusterUrl(String clusterUrl) {
      this.clusterUrl = clusterUrl;
      return this;
    }

    public Builder setAppId(String appId) {
      this.appId = checkNotNull(appId, "The appId for auth must not be null");
      return this;
    }

    public Builder setAppKey(String appKey) {
      this.appKey = checkNotNull(appKey, "The app key for auth must not be null");
      return this;
    }

    public Builder setTenantId(String tenantId) {
      this.tenantId = checkNotNull(tenantId, "The app key for auth must not be null");
      return this;
    }

    public Builder setManagedIdentityAppId(String managedIdentityAppId) {
      this.managedIdentityAppId =
          checkNotNull(managedIdentityAppId, "The User managed identity for auth must not be null");
      this.isManagedIdentity = true;
      return this;
    }

    public Builder setRetryOptions(KustoRetryConfig kustoRetryConfig) {
      this.kustoRetryConfig = checkNotNull(kustoRetryConfig,
          "If KustoRetryOptions are provided, they must not be null");
      return this;
    }

    /**
     * Build the {@link KustoConnectionOptions}.
     *
     * @return a KustoConnectionOptions with the settings made for this builder.
     */
    public KustoConnectionOptions build() {
      return new KustoConnectionOptions(this);
    }
  }
}
