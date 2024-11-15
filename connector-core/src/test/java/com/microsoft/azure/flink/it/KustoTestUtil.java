package com.microsoft.azure.flink.it;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.config.KustoWriteOptions;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.skyscreamer.jsonassert.JSONCompareMode.LENIENT;

@Execution(ExecutionMode.CONCURRENT)
public class KustoTestUtil {
  private static final String KEY_COL = "vstr";
  private static final Logger LOG = LoggerFactory.getLogger(KustoTestUtil.class);

  public static void performAssertions(Client engineClient, KustoWriteOptions writeOptions,
      Map<String, String> expectedResults, int maxRecords, String typeKey) {
    try {
      // Perform the assertions here
      Map<String, String> actualRecordsIngested =
          getActualRecordsIngested(engineClient, writeOptions, maxRecords, typeKey);
      actualRecordsIngested.keySet().parallelStream().forEach(key -> {
        LOG.trace("Record queried: {} and expected record {} ", actualRecordsIngested.get(key),
            expectedResults.get(key));
        try {
          LOG.trace(
              "==============================================================================================");
          LOG.trace("Expected {} and got {}", expectedResults.get(key),
              actualRecordsIngested.get(key));
          LOG.trace(
              "==============================================================================================");
          JSONAssert.assertEquals(expectedResults.get(key), actualRecordsIngested.get(key),
              new CustomComparator(LENIENT,
                  // there are sometimes round off errors in the double values, but they are close
                  // enough to 8 precision
                  new Customization("vdec",
                      (vdec1,
                          vdec2) -> Math.abs(Double.parseDouble(vdec1.toString())
                              - Double.parseDouble(vdec2.toString())) < 0.000000001),
                  new Customization("vreal",
                      (vreal1,
                          vreal2) -> Math.abs(Double.parseDouble(vreal1.toString())
                              - Double.parseDouble(vreal2.toString())) < 0.000000001),
                  new Customization("vdate", (vdate1, vdate2) -> Instant.parse(vdate1.toString())
                      .toEpochMilli() == Instant.parse(vdate2.toString()).toEpochMilli())));
        } catch (JSONException e) {
          fail(e);
        }
      });
      assertEquals(maxRecords, actualRecordsIngested.size());
    } catch (Exception e) {
      LOG.error("Failed performing assertions in KustoSink", e);
      fail(e);
    }
  }

  private static @NotNull Map<String, String> getActualRecordsIngested(Client engineClient,
      KustoWriteOptions writeOptions, int maxRecords, String typeKey) {
    String query = String.format(
        "%s | where type == '%s'| project  %s,vresult = pack_all() | order by vstr asc ",
        writeOptions.getTable(), typeKey, KEY_COL);
    Predicate<Object> predicate = (results) -> {
      if (results != null) {
        LOG.warn("Retrieved records count {}", ((Map<?, ?>) results).size());
      }
      return results == null || ((Map<?, ?>) results).isEmpty()
          || ((Map<?, ?>) results).size() < maxRecords;
    };
    // Waits 30 seconds for the records to be ingested. Repeats the poll 5 times , in all 150
    // seconds
    RetryConfig config = RetryConfig.custom().maxAttempts(5).retryOnResult(predicate)
        .waitDuration(Duration.of(30, SECONDS)).build();
    RetryRegistry registry = RetryRegistry.of(config);
    Retry retry = registry.retry("ingestRecordService", config);
    Supplier<Map<String, String>> recordSearchSupplier = () -> {
      try {
        LOG.warn("Executing query {} ", query);
        KustoResultSetTable resultSet =
            engineClient.execute(writeOptions.getDatabase(), query).getPrimaryResults();
        Map<String, String> actualResults = new HashMap<>();
        while (resultSet.next()) {
          String key = resultSet.getString(KEY_COL);
          String vResult = resultSet.getString("vresult");
          LOG.trace("Record queried: {}", vResult);
          actualResults.put(key, vResult);
        }
        return actualResults;
      } catch (DataServiceException | DataClientException e) {
        return Collections.emptyMap();
      }
    };
    return retry.executeSupplier(recordSearchSupplier);
  }

  public static void createTables(Client engineClient, @NotNull KustoWriteOptions writeOptions)
      throws Exception {
    URL kqlResource = KustoWriteSinkWriterIT.class.getClassLoader().getResource("it-setup.kql");
    assert kqlResource != null;
    String expiryDate = Instant.now().plus(1, ChronoUnit.HOURS).toString();
    List<String> kqlsToExecute = Files.readAllLines(Paths.get(kqlResource.toURI())).stream()
        .map(kql -> kql.replace("TBL", writeOptions.getTable()).replace("EXPIRY", expiryDate))
        .collect(Collectors.toList());
    kqlsToExecute.forEach(kql -> {
      try {
        engineClient.execute(writeOptions.getDatabase(), kql);
      } catch (Exception e) {
        LOG.error("Failed to execute kql: {}", kql, e);
      }
    });
    LOG.info("Created table {} and associated mappings", writeOptions.getTable());
  }

  public static void refreshDm(Client dmClient, @NotNull KustoWriteOptions writeOptions)
      throws Exception {
    URL kqlResource =
        KustoWriteSinkWriterIT.class.getClassLoader().getResource("policy-refresh.kql");
    assert kqlResource != null;
    List<String> kqlsToExecute = Files.readAllLines(Paths.get(kqlResource.toURI())).stream()
        .map(kql -> kql.replace("TBL", writeOptions.getTable()))
        .map(kql -> kql.replace("DB", writeOptions.getDatabase())).collect(Collectors.toList());
    kqlsToExecute.forEach(kql -> {
      try {
        dmClient.execute(kql);
      } catch (Exception e) {
        LOG.error("Failed to execute DM kql: {}", kql, e);
      }
    });
    LOG.info("Refreshed cache on DB {}", writeOptions.getDatabase());
  }

}
