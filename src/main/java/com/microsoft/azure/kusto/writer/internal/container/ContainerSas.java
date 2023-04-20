package com.microsoft.azure.kusto.writer.internal.container;

public class ContainerSas {
  private final String containerUrl;
  private final String sasToken;

  public ContainerSas(String containerUrl, String sasToken) {
    this.containerUrl = containerUrl;
    this.sasToken = sasToken;
  }

  public String getContainerUrl() {
    return containerUrl;
  }

  public String getSasToken() {
    return sasToken;
  }
}
