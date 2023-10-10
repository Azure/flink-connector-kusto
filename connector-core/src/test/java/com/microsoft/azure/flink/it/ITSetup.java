package com.microsoft.azure.flink.it;

import java.util.UUID;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.DeliveryGuarantee;

import com.microsoft.azure.flink.common.KustoRetryConfig;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;

@Internal
public class ITSetup {
  public synchronized static KustoConnectionOptions getConnectorProperties() {
    String appId = getProperty("appId", "", false);
    String appKey = getProperty("appKey", "", false);
    String authority = getProperty("authority", "", false);
    String cluster = getProperty("cluster", "", false);
    return KustoConnectionOptions.builder().withAppId(appId).withAppKey(appKey)
        .withTenantId(authority).withClusterUrl(cluster).build();
  }

  public synchronized static KustoConnectionOptions getConnectorPropertiesWithCustomRetries(
      KustoRetryConfig retryConfig) {
    String appId = getProperty("appId", "", false);
    String appKey = getProperty("appKey", "", false);
    String authority = getProperty("authority", "", false);
    String cluster = getProperty("cluster", "", false);
    return KustoConnectionOptions.builder().withAppId(appId).withAppKey(appKey)
        .withTenantId(authority).withRetryOptions(retryConfig).withClusterUrl(cluster).build();
  }



  public static KustoWriteOptions getWriteOptions() {
    return getWriteOptions(10_000, 100);
  }

  public static KustoWriteOptions getWriteOptions(DeliveryGuarantee deliveryGuarantee) {
    return getWriteOptions(10_000, 100, deliveryGuarantee);
  }

  public static KustoWriteOptions getWriteOptions(int batchInterval, int batchSize) {
    return getWriteOptions(batchInterval, batchSize, DeliveryGuarantee.AT_LEAST_ONCE);
  }

  public static KustoWriteOptions getWriteOptions(int batchInterval, int batchSize,
      DeliveryGuarantee deliveryGuarantee) {
    String database = getProperty("database", "e2e", true);
    String defaultTable =
        String.format("tmpFlinkSinkIT_%s", UUID.randomUUID().toString().replace('-', '_'));
    String table = getProperty("table", defaultTable, true);
    return KustoWriteOptions.builder().withDatabase(database).withTable(table)
        .withDeliveryGuarantee(deliveryGuarantee).withBatchIntervalMs(batchInterval)
        .withBatchSize(batchSize).build();
  }


  private static String getProperty(String attribute, String defaultValue, boolean sanitize) {
    String value = System.getProperty(attribute);
    if (value == null) {
      value = System.getenv(attribute);
    }
    // In some cases we want a default value (for example DB name). The mandatory ones are checked
    // in the IT before set-up
    value = StringUtils.isEmpty(value) ? defaultValue : value;
    return sanitize ? FilenameUtils.normalizeNoEndSeparator(value) : value;
  }
}
