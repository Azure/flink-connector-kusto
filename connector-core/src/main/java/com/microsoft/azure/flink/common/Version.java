package com.microsoft.azure.flink.common;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Version {
  public static final String CLIENT_NAME = "Flink.Sink";
  private static final Logger log = LoggerFactory.getLogger(Version.class);
  private static final String VERSION_FILE = "/azure-kusto-flink-version.properties";
  private static String version = "unknown";

  static {
    try {
      Properties props = new Properties();
      try (InputStream versionFileStream = Version.class.getResourceAsStream(VERSION_FILE)) {
        props.load(versionFileStream);
        version = props.getProperty("version", version).trim();
      }
    } catch (Exception e) {
      log.warn("Error while loading version:", e);
    }
  }

  public static String getVersion() {
    return version;
  }
}
