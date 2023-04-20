package com.microsoft.azure.kusto.writer.internal;

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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.config.KustoConnectionOptions;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoDataExceptionBase;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.writer.internal.container.ContainerSas;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;

import static com.microsoft.azure.kusto.common.RetryConfig.*;

public class ContainerProvider implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerProvider.class);
  private static final long serialVersionUID = 1L;

  private final Random randomGenerator;

  private static final String GET_TEMP_STORAGE_CONTAINER = ".create tempstorage";

  private static final List<ContainerSas> CONTAINER_SAS =
      Collections.synchronizedList(new ArrayList<>());

  private long expirationTimestamp;

  private final KustoConnectionOptions connectionOptions;

  private transient final IntervalFunction backOffFunction = IntervalFunction
      .ofExponentialRandomBackoff(BASE_INTERVAL_MILLIS, IntervalFunction.DEFAULT_MULTIPLIER,
          IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR, MAX_INTERVAL_MILLIS);
  private transient final Predicate<Throwable> isTransientException = e -> {
    if ((e instanceof KustoDataExceptionBase)) {
      return !((KustoDataExceptionBase) e).isPermanent();
    }
    return e instanceof IngestionServiceException;
  };
  private transient final RetryConfig retryConfig = RetryConfig.custom().maxAttempts(MAX_ATTEMPTS)
      .intervalFunction(backOffFunction).retryOnException(isTransientException).build();


  public ContainerProvider(KustoConnectionOptions connectionOptions) throws URISyntaxException {
    this.connectionOptions = connectionOptions;
    this.randomGenerator = new Random();
  }

  public ContainerSas getContainer()
      throws URISyntaxException, DataServiceException, DataClientException {
    if (expirationTimestamp >= Instant.now(Clock.systemUTC()).toEpochMilli()
        || !CONTAINER_SAS.isEmpty()) {
      int index = this.randomGenerator.nextInt(CONTAINER_SAS.size());
      LOG.info("Returning storage from cache {}", CONTAINER_SAS.get(index));
      return CONTAINER_SAS.get(index);
    }
    ConnectionStringBuilder kustoIngestCsb = this.connectionOptions.isManagedIdentity()
        ? ConnectionStringBuilder.createWithAadManagedIdentity(
            this.connectionOptions.getIngestUrl(), this.connectionOptions.getManagedIdentityAppId())
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
            .plus(CACHE_EXPIRATION_MINUTES, ChronoUnit.MINUTES).toEpochMilli();
        int index = this.randomGenerator.nextInt(CONTAINER_SAS.size());
        return CONTAINER_SAS.get(index);
      }
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
