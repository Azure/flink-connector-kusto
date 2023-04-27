package com.microsoft.azure.flink.common;

import java.net.URISyntaxException;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;

public class IngestClientUtil {
  public static IngestClient createIngestClient(KustoConnectionOptions connectionOptions)
      throws URISyntaxException {
    return IngestClientFactory.createClient(getKustoConnectionStringBuilder(connectionOptions));
  }

  public static Client createClient(KustoConnectionOptions connectionOptions)
      throws URISyntaxException {
    return ClientFactory.createClient(getKustoConnectionStringBuilder(connectionOptions));
  }

  private static ConnectionStringBuilder getKustoConnectionStringBuilder(
      KustoConnectionOptions connectionOptions) {
    return connectionOptions.isManagedIdentity()
        ? ConnectionStringBuilder.createWithAadManagedIdentity(connectionOptions.getIngestUrl(),
            connectionOptions.getManagedIdentityAppId())
        : ConnectionStringBuilder.createWithAadApplicationCredentials(
            connectionOptions.getIngestUrl(), connectionOptions.getAppId(),
            connectionOptions.getAppKey(), connectionOptions.getTenantId());
  }
}
