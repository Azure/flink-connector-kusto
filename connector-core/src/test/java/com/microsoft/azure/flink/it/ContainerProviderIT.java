package com.microsoft.azure.flink.it;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.common.KustoRetryConfig;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.writer.internal.ContainerProvider;

import static com.microsoft.azure.flink.it.ITSetup.getConnectorPropertiesWithCustomRetries;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Execution(ExecutionMode.CONCURRENT)
public class ContainerProviderIT {
  private static ContainerProvider containerProvider;
  private static final Logger LOG = LoggerFactory.getLogger(ContainerProviderIT.class);

  @BeforeAll
  public static void setup() {
    // Test with a short TTL
    KustoRetryConfig queryRetryConfig =
        KustoRetryConfig.builder().withCacheExpirationSeconds(5).build();
    KustoConnectionOptions validConnectionOptions =
        getConnectorPropertiesWithCustomRetries(queryRetryConfig);
    containerProvider = new ContainerProvider.Builder(validConnectionOptions).build();
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
      LOG.debug("expirationTimestamp: {}, expirationTimestampRenewed: {}", expirationTimestamp,
          expirationTimestampRenewed);
      assertTrue(expirationTimestampRenewed >= expirationTimestamp);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
