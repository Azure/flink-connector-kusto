package com.microsoft.azure.flink.quickstart;

/**
 * POJO representing a sensor reading message from Kafka.
 *
 * <p>
 * Fields are sorted alphabetically because Flink's PojoTypeInfo uses alphabetical order for field
 * serialization. The Kusto table columns must match this order (see kusto-config/create-table.kql).
 *
 * <p>
 * Sample JSON:
 *
 * <pre>
 * {"id":4821,"timestamp":"2026-02-25T06:15:00Z","sensor":"sensor-3","value":472,"unit":"celsius","message_number":1}
 * </pre>
 */
public class SensorReading {
  private int id;
  private int message_number;
  private String sensor;
  private String timestamp;
  private String unit;
  private int value;

  public SensorReading() {}

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getMessage_number() {
    return message_number;
  }

  public void setMessage_number(int message_number) {
    this.message_number = message_number;
  }

  public String getSensor() {
    return sensor;
  }

  public void setSensor(String sensor) {
    this.sensor = sensor;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public String getUnit() {
    return unit;
  }

  public void setUnit(String unit) {
    this.unit = unit;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "SensorReading{" + "id=" + id + ", message_number=" + message_number + ", sensor='"
        + sensor + '\'' + ", timestamp='" + timestamp + '\'' + ", unit='" + unit + '\'' + ", value="
        + value + '}';
  }
}
