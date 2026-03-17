package com.microsoft.azure.flink.writer.internal.sink;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * Serializer for {@link KustoCommittable} used by Flink's two-phase commit protocol to persist
 * committables across checkpoints and restarts.
 */
@Internal
public class KustoCommittableSerializer implements SimpleVersionedSerializer<KustoCommittable> {
  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(KustoCommittable committable) throws IOException {
    try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos)) {
      out.writeUTF(committable.getContainerEndpoint());
      out.writeUTF(committable.getContainerSas());
      out.writeUTF(committable.getBlobName());
      out.writeLong(committable.getSourceId().getMostSignificantBits());
      out.writeLong(committable.getSourceId().getLeastSignificantBits());
      out.writeLong(committable.getRecordCount());
      out.flush();
      return baos.toByteArray();
    }
  }

  @Override
  public KustoCommittable deserialize(int version, byte[] serialized) throws IOException {
    if (version != VERSION) {
      throw new IOException("Unsupported KustoCommittable serialization version: " + version);
    }
    try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(serialized);
        DataInputStream in = new DataInputStream(bais)) {
      String containerEndpoint = in.readUTF();
      String containerSas = in.readUTF();
      String blobName = in.readUTF();
      long mostSigBits = in.readLong();
      long leastSigBits = in.readLong();
      UUID sourceId = new UUID(mostSigBits, leastSigBits);
      long recordCount = in.readLong();
      return new KustoCommittable(containerEndpoint, containerSas, blobName, sourceId, recordCount);
    }
  }
}
