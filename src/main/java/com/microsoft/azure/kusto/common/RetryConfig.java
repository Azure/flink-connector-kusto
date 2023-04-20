package com.microsoft.azure.kusto.common;

public class RetryConfig {
  private RetryConfig() {}

  public static final int MAX_ATTEMPTS = 3;
  public static final long BASE_INTERVAL_MILLIS = 1000L;
  public static final long MAX_INTERVAL_MILLIS = 10 * BASE_INTERVAL_MILLIS;
  public static final int CACHE_EXPIRATION_MINUTES = 120;
}
