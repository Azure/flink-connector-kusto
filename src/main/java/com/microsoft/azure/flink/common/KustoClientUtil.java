package com.microsoft.azure.flink.common;

import java.net.URISyntaxException;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;

public class KustoClientUtil {
  public static IngestClient createIngestClient(KustoConnectionOptions connectionOptions)
      throws URISyntaxException {
    return IngestClientFactory.createClient(getIngestKcsb(connectionOptions));
  }

  public static Client createDMClient(KustoConnectionOptions connectionOptions)
      throws URISyntaxException {
    return ClientFactory.createClient(getIngestKcsb(connectionOptions));
  }

  public static IngestClient createMangedIngestClient(KustoConnectionOptions connectionOptions)
      throws URISyntaxException {
    return IngestClientFactory.createManagedStreamingIngestClient(getIngestKcsb(connectionOptions));
  }

  public static Client createClient(KustoConnectionOptions connectionOptions)
      throws URISyntaxException {
    return ClientFactory.createClient(getQueryKcsb(connectionOptions));
  }

  public static ConnectionStringBuilder getIngestKcsb(KustoConnectionOptions connectionOptions) {
    return connectionOptions.isManagedIdentity()
        ? ConnectionStringBuilder.createWithAadManagedIdentity(connectionOptions.getIngestUrl(),
            connectionOptions.getManagedIdentityAppId())
        : ConnectionStringBuilder.createWithAadApplicationCredentials(
            connectionOptions.getIngestUrl(), connectionOptions.getAppId(),
            connectionOptions.getAppKey(), connectionOptions.getTenantId());
  }

  private static ConnectionStringBuilder getQueryKcsb(KustoConnectionOptions connectionOptions) {
    return connectionOptions.isManagedIdentity()
        ? ConnectionStringBuilder.createWithAadManagedIdentity(connectionOptions.getClusterUrl(),
            connectionOptions.getManagedIdentityAppId())
        : ConnectionStringBuilder.createWithAadApplicationCredentials(
            connectionOptions.getClusterUrl(), connectionOptions.getAppId(),
            connectionOptions.getAppKey(), connectionOptions.getTenantId());
  }
}
