package com.microsoft.azure.kusto.writer.serializer;

import java.io.Serializable;

public class KustoRow implements Serializable {
  private final String blobName;

  private final Object[] values;

  public KustoRow(String blobName, Object[] values) {
    this.blobName = blobName;
    this.values = values;
  }
}
