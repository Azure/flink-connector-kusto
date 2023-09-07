package com.microsoft.azure.flink.writer.internal.sink;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.flink.annotation.Internal;
import org.jetbrains.annotations.NotNull;

/**
 * This class is used to write data to blob storage in multiple volumes. The max size of each volume
 * is specified by maxByteSizePerBlob. When the size of the current volume reaches
 * maxByteSizePerBlob, a new blob will be created.
 */
@Internal
public class BlobOutputMultiVolume extends OutputStream {
  private final long maxByteSizePerBlob;
  private long bytesInCurrentVolume = 0;
  private OutputStream out;
  private final OutputStreamSupplier outputStreamSupplier;

  public interface OutputStreamSupplier {
    OutputStream get() throws IOException;
  }

  /**
   * @param maxBytePerOutput max size of each blob
   * @param outputStreamSupplier a supplier that returns an output stream
   * @throws IOException if the output stream cannot be created
   */
  public BlobOutputMultiVolume(long maxBytePerOutput,
      @NotNull OutputStreamSupplier outputStreamSupplier) throws IOException {
    this.outputStreamSupplier = outputStreamSupplier;
    this.maxByteSizePerBlob = maxBytePerOutput;
    this.out = outputStreamSupplier.get();
  }

  @Override
  public synchronized void write(byte @NotNull [] bytes) throws IOException {
    final int remainingBytesInVol = (int) (maxByteSizePerBlob - bytesInCurrentVolume);
    if (remainingBytesInVol >= bytes.length) {
      out.write(bytes);
      bytesInCurrentVolume += bytes.length;
      return;
    }
    out.write(bytes, 0, remainingBytesInVol);
    switchOutput();
    this.write(bytes, remainingBytesInVol, bytes.length - remainingBytesInVol);
  }

  @Override
  public synchronized void write(int b) throws IOException {
    if (bytesInCurrentVolume + 1 <= maxByteSizePerBlob) {
      out.write(b);
      bytesInCurrentVolume += 1;
      return;
    }
    switchOutput();
    out.write(b);
    bytesInCurrentVolume += 1;
  }

  @Override
  public synchronized void write(byte @NotNull [] b, int off, int len) throws IOException {
    final int remainingBytesInVol = (int) (maxByteSizePerBlob - bytesInCurrentVolume);
    if (remainingBytesInVol >= len) {
      out.write(b, off, len);
      bytesInCurrentVolume += len;
      return;
    }
    out.write(b, off, remainingBytesInVol);
    switchOutput();
    this.write(b, off + remainingBytesInVol, len - remainingBytesInVol);
    bytesInCurrentVolume += len - remainingBytesInVol;
  }

  private void switchOutput() throws IOException {
    out.flush();
    out.close();
    out = outputStreamSupplier.get();
    bytesInCurrentVolume = 0;
  }

  @Override
  public synchronized void close() throws IOException {
    out.close();
  }

  @Override
  public synchronized void flush() throws IOException {
    out.flush();
  }
}
