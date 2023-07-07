package com.microsoft.azure.flink.common;

import java.net.URISyntaxException;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;

public class KustoClientUtil {
  public static IngestClient createIngestClient(KustoConnectionOptions connectionOptions,
      String sourceClass) throws URISyntaxException {
    return IngestClientFactory
        .createClient(getIngestKcsb(connectionOptions, sourceClass, "ingest"));
  }

  public static Client createDMClient(KustoConnectionOptions connectionOptions, String sourceClass)
      throws URISyntaxException {
    return ClientFactory.createClient(getIngestKcsb(connectionOptions, sourceClass, "dm"));
  }

  public static IngestClient createMangedIngestClient(KustoConnectionOptions connectionOptions,
      String sourceClass) throws URISyntaxException {
    return IngestClientFactory.createManagedStreamingIngestClient(
        getIngestKcsb(connectionOptions, sourceClass, "ingest"));
  }

  public static Client createClient(KustoConnectionOptions connectionOptions, String sourceClass)
      throws URISyntaxException {
    return ClientFactory.createClient(getQueryKcsb(connectionOptions, sourceClass));
  }


  public static ConnectionStringBuilder getIngestKcsb(KustoConnectionOptions connectionOptions,
      String sourceClass, String clusterType) {
    ConnectionStringBuilder kcsb = connectionOptions.isManagedIdentity()
        ? ConnectionStringBuilder.createWithAadManagedIdentity(connectionOptions.getIngestUrl(),
            connectionOptions.getManagedIdentityAppId())
        : ConnectionStringBuilder.createWithAadApplicationCredentials(
            connectionOptions.getIngestUrl(), connectionOptions.getAppId(),
            connectionOptions.getAppKey(), connectionOptions.getTenantId());
    Pair<String, String> sinkTag = ImmutablePair.of("sinkType", sourceClass);
    Pair<String, String> clusterTypeTag = ImmutablePair.of("clusterType", clusterType);
    setConnectorDetails(kcsb, sinkTag, clusterTypeTag);
    return kcsb;
  }

  @SafeVarargs
  static private void setConnectorDetails(@NotNull ConnectionStringBuilder kcsb,
      Pair<String, String>... additionalOptions) {
    kcsb.setConnectorDetails(Version.CLIENT_NAME, Version.getVersion(), null, null, false, null,
        additionalOptions);

  }

  private static ConnectionStringBuilder getQueryKcsb(
      @NotNull KustoConnectionOptions connectionOptions, String sourceClass) {
    ConnectionStringBuilder kcsb = connectionOptions.isManagedIdentity()
        ? ConnectionStringBuilder.createWithAadManagedIdentity(connectionOptions.getClusterUrl(),
            connectionOptions.getManagedIdentityAppId())
        : ConnectionStringBuilder.createWithAadApplicationCredentials(
            connectionOptions.getClusterUrl(), connectionOptions.getAppId(),
            connectionOptions.getAppKey(), connectionOptions.getTenantId());
    Pair<String, String> sinkTag = ImmutablePair.of("sinkType", sourceClass);
    Pair<String, String> clusterTypeTag = ImmutablePair.of("clusterType", "queued");
    setConnectorDetails(kcsb, sinkTag, clusterTypeTag);
    return kcsb;
  }
}
