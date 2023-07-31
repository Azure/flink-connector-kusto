package com.microsoft.azure.flink.writer.internal.container;

import org.apache.flink.annotation.Internal;

@Internal
public class ContainerSas {
  private final String containerUrl;
  private final String sasToken;

  public ContainerSas(String containerUrl, String sasToken) {
    this.containerUrl = containerUrl;
    this.sasToken = sasToken;
  }

  /**
   * Get the container url.
   *
   * @return container url
   */
  public String getContainerUrl() {
    return containerUrl;
  }

  /**
   * Get the sas token.
   *
   * @return sas token
   */
  public String getSasToken() {
    return sasToken;
  }

  @Override
  public String toString() {
    return String.format("%s?%s", containerUrl, sasToken);
  }
}
