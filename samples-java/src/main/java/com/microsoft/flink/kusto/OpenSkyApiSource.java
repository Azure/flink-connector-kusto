package com.microsoft.flink.kusto;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.opensky.api.OpenSkyApi;
import org.opensky.model.OpenSkyStates;
import org.opensky.model.StateVector;

public class OpenSkyApiSource implements SourceFunction<StateVector> {
  private volatile boolean isRunning = true;

  @Override
  public void run(SourceContext<StateVector> sourceContext) throws Exception {
    while (isRunning) {
      OpenSkyApi api = new OpenSkyApi("", "");
      OpenSkyStates os = api.getStates(0, null,
          new OpenSkyApi.BoundingBox(37.6738930354, 6.5531170279, 97.4025614766, 67.954441309));
      os.getStates().parallelStream().forEach(sourceContext::collect);
    }
    Thread.sleep(2 * 60 * 1000);
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}
