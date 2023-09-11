package com.microsoft.flink.kusto.cryptorates.common;

public class Heartbeat {
  private String type;
  private String last_trade_id;

  private String product_id;

  private Long sequence;
  private String time;

  private String processing_dttm;

  public String getProcessing_dttm() {
    return processing_dttm;
  }

  public void setProcessing_dttm(String processing_dttm) {
    this.processing_dttm = processing_dttm;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getLast_trade_id() {
    return last_trade_id;
  }

  public void setLast_trade_id(String last_trade_id) {
    this.last_trade_id = last_trade_id;
  }

  public String getProduct_id() {
    return product_id;
  }

  public void setProduct_id(String product_id) {
    this.product_id = product_id;
  }

  public Long getSequence() {
    return sequence;
  }

  public void setSequence(Long sequence) {
    this.sequence = sequence;
  }

  public String getTime() {
    return time;
  }

  public void setTime(String time) {
    this.time = time;
  }

}
