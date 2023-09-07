package com.microsoft.flink.kusto.cryptorates;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class BTCWebSocketSource extends RichSourceFunction<String> {
  private static final Logger LOG = LoggerFactory.getLogger(BTCWebSocketSource.class);
  private final BlockingQueue<String> messagesQueue = new LinkedBlockingQueue<>(100000);
  private boolean running = true;

  @Override
  public void run(SourceContext<String> sourceContext) throws Exception {
    while (running) {
      String message = messagesQueue.take();
      sourceContext.collect(message);
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    AsyncHttpClient asyncHttpClient = asyncHttpClient();
    asyncHttpClient.prepareGet("wss://ws-feed.exchange.coinbase.com").execute(
        new WebSocketUpgradeHandler.Builder().addWebSocketListener(new WebSocketListener() {
          @Override
          public void onOpen(WebSocket webSocket) {
            webSocket.sendTextFrame("{\n" + "    \"type\": \"subscribe\",\n"
                + "    \"product_ids\": [\n" + "        \"ETH-USD\",\n" + "        \"ETH-EUR\"\n"
                + "    ],\n" + "    \"channels\": [\n" + "        \"level2\",\n"
                + "        \"heartbeat\",\n" + "        {\n" + "            \"name\": \"ticker\",\n"
                + "            \"product_ids\": [\n" + "                \"ETH-BTC\",\n"
                + "                \"ETH-USD\"\n" + "            ]\n" + "        }\n" + "    ]\n"
                + "}");
            LOG.info("Subscribed to ETH-USD and ETH-EUR");
          }

          @Override
          public void onClose(WebSocket webSocket, int i, String s) {
            webSocket.sendCloseFrame();
          }

          @Override
          public void onError(Throwable throwable) {
            throw new RuntimeException(throwable);
          }

          @Override
          public void onTextFrame(String payload, boolean finalFragment, int rsv) {
            messagesQueue.add(payload);
          }
        }).build()).get();
  }
}
