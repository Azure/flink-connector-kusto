package com.microsoft.azure.flink.flink;

import java.util.UUID;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.config.KustoWriteOptions;

public class ITSetup {
  public static KustoConnectionOptions getConnectorProperties() {
    String testPrefix = "tmpKafkaSinkIT_";
    String appId = getProperty("appId", "", false);
    String appKey = getProperty("appKey", "", false);
    String authority = getProperty("authority", "", false);
    String cluster = getProperty("cluster", "", false);
    String defaultTable = testPrefix + UUID.randomUUID().toString().replace('-', '_');
    return KustoConnectionOptions.builder().setAppId(appId).setAppKey(appKey).setTenantId(authority)
        .setClusterUrl(cluster).build();
  }

  public static KustoWriteOptions getWriteOptions() {
    String database = getProperty("database", "e2e", true);
    String defaultTable =
        String.format("tmpFlinkSinkIT_%s", UUID.randomUUID().toString().replace('-', '_'));
    String table = getProperty("table", defaultTable, true);
    return KustoWriteOptions.builder().withDatabase(database).withTable(table)
        .withBatchIntervalMs(-1).withBatchSize(100).build(); // TODO check the -1 batch interval
                                                             // value
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
