package com.microsoft.azure.flink.writer.internal;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.common.QueryRetryConfig;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.writer.internal.container.ContainerSas;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoDataExceptionBase;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

public class ContainerProvider implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerProvider.class);
  private static final long serialVersionUID = 1L;

  private final Random randomGenerator;

  private static final String GET_TEMP_STORAGE_CONTAINER = ".create tempstorage";

  private static final List<ContainerSas> CONTAINER_SAS =
      Collections.synchronizedList(new ArrayList<>());

  private long expirationTimestamp;

  private final KustoConnectionOptions connectionOptions;
  private final QueryRetryConfig queryRetryConfig;


  private transient final Retry retry;

  public ContainerProvider(KustoConnectionOptions connectionOptions,
      QueryRetryConfig queryRetryConfig) {
    this.connectionOptions = connectionOptions;
    this.randomGenerator = new Random();
    this.queryRetryConfig = queryRetryConfig;
    retry = getRetry();
  }

  @NotNull
  private Retry getRetry() {
    final Retry retry;
    IntervalFunction backOffFunction = IntervalFunction.ofExponentialRandomBackoff(
        queryRetryConfig.getBaseIntervalMillis(), IntervalFunction.DEFAULT_MULTIPLIER,
        IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR, queryRetryConfig.getMaxIntervalMillis());
    Predicate<Throwable> isTransientException = e -> {
      if ((e instanceof KustoDataExceptionBase)) {
        return !((KustoDataExceptionBase) e).isPermanent();
      }
      return e instanceof IngestionServiceException;
    };
    RetryConfig retryConfig = RetryConfig.custom().maxAttempts(queryRetryConfig.getMaxAttempts())
        .intervalFunction(backOffFunction).retryOnException(isTransientException).build();
    RetryRegistry registry = RetryRegistry.of(retryConfig);
    retry = registry.retry("tempStoreService", retryConfig);
    return retry;
  }

  public ContainerSas getBlobContainer() {
    // Not expired and a double check the list is not empty
    if (expirationTimestamp >= Instant.now(Clock.systemUTC()).toEpochMilli()
        && !CONTAINER_SAS.isEmpty()) {
      int index = this.randomGenerator.nextInt(CONTAINER_SAS.size());
      LOG.info("Returning storage from cache {}", CONTAINER_SAS.get(index));
      return CONTAINER_SAS.get(index);
    }
    return retry.executeSupplier(getContainerSupplier());
  }

  private Supplier<ContainerSas> getContainerSupplier() {
    return () -> {
      ConnectionStringBuilder kustoIngestCsb = this.connectionOptions.isManagedIdentity()
          ? ConnectionStringBuilder.createWithAadManagedIdentity(
              this.connectionOptions.getIngestUrl(),
              this.connectionOptions.getManagedIdentityAppId())
          : ConnectionStringBuilder.createWithAadApplicationCredentials(
              this.connectionOptions.getIngestUrl(), this.connectionOptions.getAppId(),
              this.connectionOptions.getAppKey(), connectionOptions.getTenantId());
      try (Client ingestClient = ClientFactory.createClient(kustoIngestCsb)) {
        CONTAINER_SAS.clear();
        KustoOperationResult queryResult = ingestClient.execute(GET_TEMP_STORAGE_CONTAINER);
        if (queryResult != null && queryResult.getPrimaryResults() != null) {
          queryResult.getPrimaryResults().getData().stream()
              .filter(row -> row.size() > 0 && row.get(0) != null
                  && StringUtils.isNotEmpty(row.get(0).toString()))
              .map(row -> row.get(0).toString().split("\\?")).forEach(parts -> {
                LOG.info("Adding container post refresh {}", parts[0]);
                CONTAINER_SAS.add(new ContainerSas(parts[0], parts[1]));
              });
          this.expirationTimestamp = Instant.now(Clock.systemUTC())
              .plus(queryRetryConfig.getCacheExpirationSeconds(), ChronoUnit.SECONDS)
              .toEpochMilli();
          LOG.info("Setting expiration timestamp to {}", this.expirationTimestamp);
          int index = this.randomGenerator.nextInt(CONTAINER_SAS.size());
          return CONTAINER_SAS.get(index);
        }
        return null;
      } catch (IOException | DataServiceException | DataClientException | URISyntaxException e) {
        LOG.error("Failed to get temp storage container", e);
        throw new RuntimeException(e);
      }
    };
  }
  public long getExpirationTimestamp() {
    return expirationTimestamp;
  }
}
