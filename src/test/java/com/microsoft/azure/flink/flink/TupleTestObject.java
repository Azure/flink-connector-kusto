package com.microsoft.azure.flink.flink;

import java.time.Clock;
import java.time.Instant;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple8;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TupleTestObject {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private int vnum;
  private double vdec;
  private String vdate;
  private boolean vb;
  private double vreal;
  private String vstr;
  private long vlong;

  public int getVnum() {
    return vnum;
  }

  public double getVdec() {
    return vdec;
  }

  public String getVdate() {
    return vdate;
  }

  public boolean isVb() {
    return vb;
  }

  public double getVreal() {
    return vreal;
  }

  public String getVstr() {
    return vstr;
  }

  public long getVlong() {
    return vlong;
  }

  public String getType() {
    return type;
  }

  private String type;

  public TupleTestObject(int iterationKey) {
    // randomize values with Random
    Random rand = new Random();
    this.vnum = rand.nextInt(100);
    this.vdec = rand.nextDouble();
    this.vdate = Instant.now(Clock.systemUTC()).plusSeconds(iterationKey).toString();// Take a
                                                                                     // random date
                                                                                     // for test
    this.vb = rand.nextBoolean();
    this.vreal = rand.nextDouble();
    this.vstr = String.format("Flink,,;@#-%s", iterationKey);
    this.vlong = rand.nextLong();
    this.type = "Flink-Tuple-Test";
  }

  public String toJsonString() throws Exception {
    return MAPPER.writeValueAsString(this);
  }

  public Tuple8<Integer, Double, String, Boolean, Double, String, Long, String> toTuple() {
    return new Tuple8<>(this.vnum, this.vdec, this.vdate, this.vb, this.vreal, this.vstr,
        this.vlong, this.type);
  }
}
