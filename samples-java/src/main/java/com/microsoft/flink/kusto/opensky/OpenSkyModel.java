package com.microsoft.flink.kusto.opensky;

import java.io.Serializable;

public class OpenSkyModel implements Serializable {
  public String icao24;
  public long firstSeen;
  public String estDepartureAirport;
  public long lastSeen;
  public String estArrivalAirport;
  public String callsign;
  public int estDepartureAirportHorizDistance;
  public int estDepartureAirportVertDistance;
  public int estArrivalAirportHorizDistance;
  public int estArrivalAirportVertDistance;
  public int departureAirportCandidatesCount;
  public int arrivalAirportCandidatesCount;

  public String getIcao24() {
    return icao24;
  }

  public void setIcao24(String icao24) {
    this.icao24 = icao24;
  }

  public long getFirstSeen() {
    return firstSeen;
  }

  public void setFirstSeen(long firstSeen) {
    this.firstSeen = firstSeen;
  }

  public String getEstDepartureAirport() {
    return estDepartureAirport;
  }

  public void setEstDepartureAirport(String estDepartureAirport) {
    this.estDepartureAirport = estDepartureAirport;
  }

  public long getLastSeen() {
    return lastSeen;
  }

  public void setLastSeen(long lastSeen) {
    this.lastSeen = lastSeen;
  }

  public String getEstArrivalAirport() {
    return estArrivalAirport;
  }

  public void setEstArrivalAirport(String estArrivalAirport) {
    this.estArrivalAirport = estArrivalAirport;
  }

  public String getCallsign() {
    return callsign;
  }

  public void setCallsign(String callsign) {
    this.callsign = callsign;
  }

  public int getEstDepartureAirportHorizDistance() {
    return estDepartureAirportHorizDistance;
  }

  public void setEstDepartureAirportHorizDistance(int estDepartureAirportHorizDistance) {
    this.estDepartureAirportHorizDistance = estDepartureAirportHorizDistance;
  }

  public int getEstDepartureAirportVertDistance() {
    return estDepartureAirportVertDistance;
  }

  public void setEstDepartureAirportVertDistance(int estDepartureAirportVertDistance) {
    this.estDepartureAirportVertDistance = estDepartureAirportVertDistance;
  }

  public int getEstArrivalAirportHorizDistance() {
    return estArrivalAirportHorizDistance;
  }

  public void setEstArrivalAirportHorizDistance(int estArrivalAirportHorizDistance) {
    this.estArrivalAirportHorizDistance = estArrivalAirportHorizDistance;
  }

  public int getEstArrivalAirportVertDistance() {
    return estArrivalAirportVertDistance;
  }

  public void setEstArrivalAirportVertDistance(int estArrivalAirportVertDistance) {
    this.estArrivalAirportVertDistance = estArrivalAirportVertDistance;
  }

  public int getDepartureAirportCandidatesCount() {
    return departureAirportCandidatesCount;
  }

  public void setDepartureAirportCandidatesCount(int departureAirportCandidatesCount) {
    this.departureAirportCandidatesCount = departureAirportCandidatesCount;
  }

  public int getArrivalAirportCandidatesCount() {
    return arrivalAirportCandidatesCount;
  }

  public void setArrivalAirportCandidatesCount(int arrivalAirportCandidatesCount) {
    this.arrivalAirportCandidatesCount = arrivalAirportCandidatesCount;
  }
}
