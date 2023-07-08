package com.microsoft.flink.kusto;

import java.time.Instant;

import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.opensky.api.OpenSkyApi;
import org.opensky.model.OpenSkyStates;

public class OpenSkyApiSource implements
    SourceFunction<Tuple17<Double, Double, Double, Double, Double, Double, String, String, Boolean, Double, Double, String, String, Boolean, Double, String, String>> {
  private volatile boolean isRunning = true;

  @Override
  public void run(
      SourceContext<Tuple17<Double, Double, Double, Double, Double, Double, String, String, Boolean, Double, Double, String, String, Boolean, Double, String, String>> sourceContext)
      throws Exception {
    while (isRunning) {
      OpenSkyApi api = new OpenSkyApi("", "");
      OpenSkyStates os = api.getStates((int) Instant.now().getEpochSecond(), null,
          new OpenSkyApi.BoundingBox(28.5363884063, 28.5766461538, 77.0604428463, 77.1289357357));
      os.getStates().parallelStream().map(stateVector -> {
        // StateVector is not a true POJO, so we need to convert it to a tuple for serialization.
        String stateVectorString =
            stateVector.getSerials() == null ? "" : stateVector.getSerials().toString();
        return new Tuple17<>(stateVector.getGeoAltitude(), stateVector.getLongitude(),
            stateVector.getLatitude(), stateVector.getVelocity(), stateVector.getHeading(),
            stateVector.getVerticalRate(), stateVector.getIcao24(), stateVector.getCallsign(),
            stateVector.isOnGround(), stateVector.getLastContact(),
            stateVector.getLastPositionUpdate(), stateVector.getOriginCountry(),
            stateVector.getSquawk(), stateVector.isSpi(), stateVector.getBaroAltitude(),
            stateVector.getPositionSource().name(), stateVectorString);
      }).forEach(sourceContext::collect);
    }
    Thread.sleep(2 * 60 * 1000);
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}
