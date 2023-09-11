package com.microsoft.flink.kusto.cryptorates.common;

public class Ticker {
  private String type;
  private Long trade_id;
  private Long sequence;
  private String time;
  private String product_id;
  private Double price;
  private String side;
  private Double last_size;
  private Double best_bid;
  private Double best_ask;
  private Double open_24h;
  private Double volume_24h;
  private Double low_24h;
  private Double high_24h;
  private Double volume_30d;
  private Double best_bid_size;
  private Double best_ask_size;

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

  public Long getTrade_id() {
    return trade_id;
  }

  public void setTrade_id(Long trade_id) {
    this.trade_id = trade_id;
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

  public String getProduct_id() {
    return product_id;
  }

  public void setProduct_id(String product_id) {
    this.product_id = product_id;
  }

  public Double getPrice() {
    return price;
  }

  public void setPrice(Double price) {
    this.price = price;
  }

  public String getSide() {
    return side;
  }

  public void setSide(String side) {
    this.side = side;
  }

  public Double getLast_size() {
    return last_size;
  }

  public void setLast_size(Double last_size) {
    this.last_size = last_size;
  }

  public Double getBest_bid() {
    return best_bid;
  }

  public void setBest_bid(Double best_bid) {
    this.best_bid = best_bid;
  }

  public Double getBest_ask() {
    return best_ask;
  }

  public void setBest_ask(Double best_ask) {
    this.best_ask = best_ask;
  }

  public Double getOpen_24h() {
    return open_24h;
  }

  public void setOpen_24h(Double open_24h) {
    this.open_24h = open_24h;
  }

  public Double getVolume_24h() {
    return volume_24h;
  }

  public void setVolume_24h(Double volume_24h) {
    this.volume_24h = volume_24h;
  }

  public Double getLow_24h() {
    return low_24h;
  }

  public void setLow_24h(Double low_24h) {
    this.low_24h = low_24h;
  }

  public Double getHigh_24h() {
    return high_24h;
  }

  public void setHigh_24h(Double high_24h) {
    this.high_24h = high_24h;
  }

  public Double getVolume_30d() {
    return volume_30d;
  }

  public void setVolume_30d(Double volume_30d) {
    this.volume_30d = volume_30d;
  }

  public Double getBest_bid_size() {
    return best_bid_size;
  }

  public void setBest_bid_size(Double best_bid_size) {
    this.best_bid_size = best_bid_size;
  }

  public Double getBest_ask_size() {
    return best_ask_size;
  }

  public void setBest_ask_size(Double best_ask_size) {
    this.best_ask_size = best_ask_size;
  }

}
