package com.microsoft.azure.flink.writer.internal.sink;

import java.io.Serializable;
import java.util.UUID;

import org.apache.flink.annotation.Internal;

/**
 * A committable representing a blob that has been written and needs to be ingested into Kusto. This
 * is the unit of work passed from the writer (phase 1) to the committer (phase 2) in the
 * two-phase commit protocol.
 */
@Internal
public class KustoCommittable implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String containerEndpoint;
  private final String containerSas;
  private final String blobName;
  private final UUID sourceId;
  private final long recordCount;

  public KustoCommittable(String containerEndpoint, String containerSas, String blobName,
      UUID sourceId, long recordCount) {
    this.containerEndpoint = containerEndpoint;
    this.containerSas = containerSas;
    this.blobName = blobName;
    this.sourceId = sourceId;
    this.recordCount = recordCount;
  }

  public String getContainerEndpoint() {
    return containerEndpoint;
  }

  public String getContainerSas() {
    return containerSas;
  }

  public String getBlobName() {
    return blobName;
  }

  public UUID getSourceId() {
    return sourceId;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public String getBlobUri() {
    return String.format("%s/%s%s", containerEndpoint, blobName, containerSas);
  }

  @Override
  public String toString() {
    return "KustoCommittable{" + "blobName='" + blobName + '\'' + ", sourceId=" + sourceId
        + ", recordCount=" + recordCount + '}';
  }
}
