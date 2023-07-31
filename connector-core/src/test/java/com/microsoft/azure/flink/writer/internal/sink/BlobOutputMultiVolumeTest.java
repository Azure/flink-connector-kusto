package com.microsoft.azure.flink.writer.internal.sink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertTrue;

public class BlobOutputMultiVolumeTest {
  private static final UUID RAND_UUID = UUID.randomUUID();
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

  @Test
  public void testRollOver() throws Exception {
    final int kbsToWrite = 21;
    final long maxBytePerOutput = kbsToWrite * 1024; /* 21 KB of data */
    final int numBytes = 1024; // 1KB

    AtomicInteger idx = new AtomicInteger(0);
    Map<Integer, Integer> newLines = new HashMap<>();
    try (BlobOutputMultiVolume blobOutputMultiVolume =
        new BlobOutputMultiVolume(maxBytePerOutput, () -> {
          String blobNameWithIndex =
              String.format("%s-%s-%s-%s.csv.gz", "DB", "TBL", RAND_UUID, idx.getAndIncrement());
          Path tempFileOutputFilePath = Paths.get(Paths.get(TMP_DIR, blobNameWithIndex).toString());
          return new GZIPOutputStream(Files.newOutputStream(tempFileOutputFilePath));
        })) {
      for (int byteLines = 0; byteLines < 21 * 2 + 2; byteLines++) {
        SecureRandom random = new SecureRandom();
        byte[] bytes = new byte[numBytes];
        random.nextBytes(bytes);
        blobOutputMultiVolume.write(bytes);
        blobOutputMultiVolume.write(System.lineSeparator().getBytes());
        if (newLines.containsKey(idx.get())) {
          newLines.computeIfPresent(idx.get(), (k, v) -> v + 1);
        } else {
          newLines.put(idx.get(), 1);
        }
      }
      // 3 files 0,1 each of 21 KB and the carryover file of 2 KB
      Assertions.assertEquals(3, idx.get());
      for (int i = 0; i < 3; i++) {
        String fileToVerify = String.format("%s-%s-%s-%s.csv.gz", "DB", "TBL", RAND_UUID, i);
        Path tempFileOutputFilePath = Paths.get(Paths.get(TMP_DIR, fileToVerify).toString());
        Assertions.assertTrue(Files.exists(tempFileOutputFilePath),
            "File " + tempFileOutputFilePath + " does not exist");
        if (i < 2) {
          long fileSize = FileUtils.sizeOf(tempFileOutputFilePath.toFile());
          int newLineBytes =
              newLines.getOrDefault(i + 1, 0) * System.lineSeparator().getBytes().length;
          Assertions.assertEquals(maxBytePerOutput, (fileSize - newLineBytes), 50,
              "File " + tempFileOutputFilePath + " has size " + fileSize + " but expected "
                  + maxBytePerOutput);
        }
      }
    }
  }
}
