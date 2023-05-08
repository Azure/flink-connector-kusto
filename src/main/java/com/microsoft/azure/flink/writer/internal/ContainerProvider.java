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
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.common.KustoClientUtil;
import com.microsoft.azure.flink.common.KustoRetryConfig;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.flink.writer.internal.container.ContainerSas;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import io.github.resilience4j.retry.Retry;

import static com.microsoft.azure.flink.common.KustoRetryUtil.getRetries;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ContainerProvider implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerProvider.class);
  private static final long serialVersionUID = 1L;
  private final Random randomGenerator;
  private static final String GET_TEMP_STORAGE_CONTAINER = ".create tempstorage";
  private static final List<ContainerSas> CONTAINER_SAS =
      Collections.synchronizedList(new ArrayList<>());
  private long expirationTimestamp;
  private final KustoConnectionOptions connectionOptions;
  private final KustoRetryConfig kustoRetryConfig;
  private static ContainerProvider containerProviderInstance;
  private transient final Retry retry;

  private ContainerProvider(KustoConnectionOptions connectionOptions,
      KustoRetryConfig kustoRetryConfig) {
    this.connectionOptions = connectionOptions;
    this.randomGenerator = new Random();
    this.kustoRetryConfig = kustoRetryConfig;
    retry = getRetries(kustoRetryConfig);
  }

  private static synchronized ContainerProvider build(ContainerProvider.Builder builder) {
    if (containerProviderInstance == null) {
      containerProviderInstance =
          new ContainerProvider(builder.connectionOptions, builder.kustoRetryConfig);
    }
    return containerProviderInstance;
  }

  public ContainerProvider getInstance() {
    return containerProviderInstance;
  }

  public ContainerSas getBlobContainer() {
    // Not expired and a double check the list is not empty
    if (isCacheExpired()) {
      int index = this.randomGenerator.nextInt(CONTAINER_SAS.size());
      LOG.info("Returning storage from cache {}", CONTAINER_SAS.get(index));
      return CONTAINER_SAS.get(index);
    }
    return retry.executeSupplier(getContainerSupplier());
  }

  private boolean isCacheExpired() {
    return expirationTimestamp >= Instant.now(Clock.systemUTC()).toEpochMilli()
        && !CONTAINER_SAS.isEmpty();
  }

  private Supplier<ContainerSas> getContainerSupplier() {
    return () -> {
      try (Client ingestClient = KustoClientUtil.createDMClient(checkNotNull(this.connectionOptions,
          "Connection options passed to DM client cannot be null."))) {
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
              .plus(kustoRetryConfig.getCacheExpirationSeconds(), ChronoUnit.SECONDS)
              .toEpochMilli();
          LOG.info("Setting expiration timestamp to {}", this.expirationTimestamp);
          int index = this.randomGenerator.nextInt(CONTAINER_SAS.size());
          return CONTAINER_SAS.get(index);
        }
        return null;
      } catch (IOException | DataServiceException | DataClientException | URISyntaxException e) {
        LOG.error("Failed to get temp storage container", e);
        if (CONTAINER_SAS.isEmpty()) {
          throw new RuntimeException(e);
        } else {
          LOG.warn("Failed to get temp storage container. "
              + "To cover for transient failures, returning an existing container."
              + "If the SAS keys have expired, the flow will fail further in the flow", e);
          int index = this.randomGenerator.nextInt(CONTAINER_SAS.size());
          return CONTAINER_SAS.get(index);
        }
      }
    };
  }

  public long getExpirationTimestamp() {
    return expirationTimestamp;
  }


  public static class Builder {
    private final KustoConnectionOptions connectionOptions;
    private KustoRetryConfig kustoRetryConfig = new KustoRetryConfig.Builder().build();

    public Builder(KustoConnectionOptions connectionOptions) {
      this.connectionOptions = connectionOptions;
    }

    public Builder withKustoRetryConfig(KustoRetryConfig kustoRetryConfig) {
      this.kustoRetryConfig = kustoRetryConfig;
      return this;
    }

    public ContainerProvider build() {
      return ContainerProvider.build(this);
    }
  }
}
