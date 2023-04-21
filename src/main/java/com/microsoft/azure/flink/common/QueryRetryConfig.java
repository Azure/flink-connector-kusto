package com.microsoft.azure.flink.common;

public class QueryRetryConfig {
  private final int maxAttempts;

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public long getBaseIntervalMillis() {
    return baseIntervalMillis;
  }

  public long getMaxIntervalMillis() {
    return maxIntervalMillis;
  }

  public int getCacheExpirationSeconds() {
    return cacheExpirationSeconds;
  }

  private final long baseIntervalMillis;
  private final long maxIntervalMillis;
  private final int cacheExpirationSeconds;

  private QueryRetryConfig(Builder builder) {
    this.maxAttempts = builder.maxAttempts;
    this.baseIntervalMillis = builder.baseIntervalMillis;
    this.maxIntervalMillis = builder.maxIntervalMillis;
    this.cacheExpirationSeconds = builder.cacheExpirationSeconds;
  }


  public static QueryRetryConfig.Builder builder() {
    return new QueryRetryConfig.Builder();
  }

  // Builder for RetryConfig
  public static class Builder {

    private int maxAttempts = 3;
    private long baseIntervalMillis = 1000L;
    private long maxIntervalMillis = 10 * baseIntervalMillis;
    private int cacheExpirationSeconds = 120 * 60; // 2 hours

    public Builder withMaxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder withBaseIntervalMillis(long baseIntervalMillis) {
      this.baseIntervalMillis = baseIntervalMillis;
      return this;
    }

    public Builder withMaxIntervalMillis(long maxIntervalMillis) {
      this.maxIntervalMillis = maxIntervalMillis;
      return this;
    }

    public Builder withCacheExpirationSeconds(int cacheExpirationSeconds) {
      this.cacheExpirationSeconds = cacheExpirationSeconds;
      return this;
    }

    public QueryRetryConfig build() {
      return new QueryRetryConfig(this);
    }
  }
}
