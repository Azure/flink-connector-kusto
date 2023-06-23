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

  public void setVnum(int vnum) {
    this.vnum = vnum;
  }

  public void setVdec(double vdec) {
    this.vdec = vdec;
  }

  public void setVdate(String vdate) {
    this.vdate = vdate;
  }

  public void setVb(boolean vb) {
    this.vb = vb;
  }

  public void setVreal(double vreal) {
    this.vreal = vreal;
  }

  public void setVstr(String vstr) {
    this.vstr = vstr;
  }

  public void setVlong(long vlong) {
    this.vlong = vlong;
  }

  public void setType(String type) {
    this.type = type;
  }

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

  public TupleTestObject(int iterationKey, String typeKey) {
    // randomize values with Random
    Random rand = new Random();
    this.vnum = iterationKey;
    this.vdec = rand.nextDouble();
    this.vdate = Instant.now(Clock.systemUTC()).plusSeconds(iterationKey).toString();// Take a
                                                                                     // random date
                                                                                     // for test
    this.vb = rand.nextBoolean();
    this.vreal = rand.nextDouble();
    this.vstr = String.format("Flink,,;@#-%s", iterationKey);
    this.vlong = rand.nextLong();
    this.type = typeKey;
  }

  public TupleTestObject() {}

  public String toJsonString() throws Exception {
    return MAPPER.writeValueAsString(this);
  }

  public Tuple8<Integer, Double, String, Boolean, Double, String, Long, String> toTuple() {
    return new Tuple8<>(this.vnum, this.vdec, this.vdate, this.vb, this.vreal, this.vstr,
        this.vlong, this.type);
  }
}
