package com.microsoft.flink.kusto.cryptorates;

import java.time.Clock;
import java.time.Instant;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SplitTypes extends ProcessFunction<String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(SplitTypes.class);
  final OutputTag<Heartbeat> outputTagHeartbeat =
      new OutputTag<Heartbeat>("side-output-heartbeat") {};
  final OutputTag<Ticker> outputTagTicker = new OutputTag<Ticker>("side-output-ticker") {};
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void processElement(String input, ProcessFunction<String, String>.Context context,
      Collector<String> collector) throws Exception {
    // emit data to side output
    if (input.contains("subscriptions")) {
      LOG.info("Ignoring subscriptions message: {}", input);
    } else if (input.contains("heartbeat")) {
      Heartbeat result = mapper.readValue(input, Heartbeat.class);
      result.setProcessing_dttm(Instant.now(Clock.systemUTC()).toString());
      context.output(outputTagHeartbeat, result);
    } else if (input.contains("ticker")) {
      Ticker result = mapper.readValue(input, Ticker.class);
      result.setProcessing_dttm(Instant.now(Clock.systemUTC()).toString());
      context.output(outputTagTicker, result);
    } else {
      // ignore
      LOG.info("Ignoring error message: {}", input);
    }
  }
}
