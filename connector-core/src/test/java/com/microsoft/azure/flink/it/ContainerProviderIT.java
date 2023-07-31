package com.microsoft.azure.flink.it;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.microsoft.azure.flink.common.KustoRetryConfig;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.writer.internal.ContainerProvider;

import static com.microsoft.azure.flink.it.ITSetup.getConnectorProperties;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ContainerProviderIT {
  private static ContainerProvider containerProvider;

  @BeforeAll
  public static void setup() {
    KustoConnectionOptions validConnectionOptions = getConnectorProperties();
    // Test with a short TTL
    KustoRetryConfig queryRetryConfig =
        KustoRetryConfig.builder().withCacheExpirationSeconds(5).build();
    containerProvider = new ContainerProvider.Builder(validConnectionOptions)
        .withKustoRetryConfig(queryRetryConfig).build();
  }

  @Test
  public void containerSasShouldBeQueriedFromDM() {
    assertNotNull(containerProvider);
    assertNotNull(containerProvider.getBlobContainer());
    long expirationTimestamp = containerProvider.getExpirationTimestamp();
    assertTrue(containerProvider.getExpirationTimestamp() > 0);
    try {
      Thread.sleep(10000);
      assertNotNull(containerProvider.getBlobContainer());
      long expirationTimestampRenewed = containerProvider.getExpirationTimestamp();
      System.out.println("********************************************************************************");
      System.out.printf("expirationTimestamp: %d, expirationTimestampRenewed: %d%n",
          expirationTimestamp, expirationTimestampRenewed);
      assertTrue(expirationTimestampRenewed >= expirationTimestamp);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
