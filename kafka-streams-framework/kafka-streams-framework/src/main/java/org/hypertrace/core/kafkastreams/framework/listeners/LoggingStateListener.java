package org.hypertrace.core.kafkastreams.framework.listeners;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingStateListener implements KafkaStreams.StateListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingStateListener.class);
  private final KafkaStreams app;

  public LoggingStateListener(KafkaStreams app) {
    this.app = app;
  }

  @Override
  public void onChange(State newState, State oldState) {
    LOGGER.info("Transitioning from [{}] => [{}]", oldState, newState);

    if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
      LOGGER.info("Application has gone from [REBALANCING] to [RUNNING] ");
      LOGGER.info("Thread metadata [{}]", app.localThreadsMetadata());
    }

    if (newState == KafkaStreams.State.REBALANCING) {
      LOGGER.info("Application is entering [REBALANCING] phase");
    }
  }
}