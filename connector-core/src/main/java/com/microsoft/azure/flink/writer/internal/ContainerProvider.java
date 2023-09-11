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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.flink.common.KustoClientUtil;
import com.microsoft.azure.flink.common.KustoRetryConfig;
import com.microsoft.azure.flink.config.KustoConnectionOptions;
import com.microsoft.azure.kusto.ingest.QueuedIngestClient;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas;

import io.github.resilience4j.retry.Retry;

import static com.microsoft.azure.flink.common.KustoRetryUtil.getRetries;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The core for ingestion is to get a storage blob where data can be serialized and upload from
 * where data can be ingested into Kusto. This is provided for from this class. The process is to
 * get a storage account that is cached and then is used to upload data to.
 */
@Internal
@PublicEvolving
public class ContainerProvider implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerProvider.class);
  private static final long serialVersionUID = 1L;
  private final Random randomGenerator;
  private static final List<ContainerWithSas> CONTAINER_SAS =
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
      containerProviderInstance = new ContainerProvider(checkNotNull(builder.connectionOptions),
          checkNotNull(builder.kustoRetryConfig));
    }
    return containerProviderInstance;
  }

  /**
   * Returns a container with SAS key to upload data to.
   * 
   * @return Container with SAS key to upload data to.
   */
  public ContainerWithSas getBlobContainer() {
    // Not expired and a double check the list is not empty
    if (isCacheExpired()) {
      int index = this.randomGenerator.nextInt(CONTAINER_SAS.size());
      LOG.info("Returning storage from cache {}", CONTAINER_SAS.get(index).getEndpointWithoutSas());
      return CONTAINER_SAS.get(index);
    }
    return retry.executeSupplier(getContainerSupplier());
  }

  private boolean isCacheExpired() {
    return expirationTimestamp >= Instant.now(Clock.systemUTC()).toEpochMilli()
        && !CONTAINER_SAS.isEmpty();
  }

  /**
   * Returns a supplier that returns a container with SAS key to upload data to.
   * 
   * @return Supplier that returns a container with SAS key to upload data to.
   */
  @Contract(pure = true)
  private @NotNull Supplier<ContainerWithSas> getContainerSupplier() {
    return () -> {
      try (QueuedIngestClient ingestClient = KustoClientUtil.createDMClient(
          checkNotNull(this.connectionOptions,
              "Connection options passed to DM client cannot be null."),
          ContainerProvider.class.getSimpleName())) {
        CONTAINER_SAS.clear();
        this.expirationTimestamp = Instant.now(Clock.systemUTC())
            .plus(kustoRetryConfig.getCacheExpirationSeconds(), ChronoUnit.SECONDS).toEpochMilli();
        LOG.info("Setting expiration timestamp to {}", this.expirationTimestamp);
        ingestClient.getResourceManager().getShuffledContainers().forEach(container -> {
          LOG.debug("Adding container post refresh {}", container.getEndpointWithoutSas());
          CONTAINER_SAS.add(container);
        });
        int index = this.randomGenerator.nextInt(CONTAINER_SAS.size());
        return CONTAINER_SAS.get(index);
      } catch (IOException | URISyntaxException | IngestionClientException
          | IngestionServiceException e) {
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

  /**
   * Returns the expiration timestamp for the container.
   * 
   * @return Expiration timestamp for the container.
   */
  public long getExpirationTimestamp() {
    return expirationTimestamp;
  }

  /**
   * Builder for ContainerProvider.
   */

  public static class Builder {
    private final KustoConnectionOptions connectionOptions;
    private final KustoRetryConfig kustoRetryConfig;

    public Builder(@NotNull KustoConnectionOptions connectionOptions) {
      this.connectionOptions = connectionOptions;
      this.kustoRetryConfig = connectionOptions.getKustoRetryConfig();
    }

    public ContainerProvider build() {
      return ContainerProvider.build(this);
    }
  }
}
