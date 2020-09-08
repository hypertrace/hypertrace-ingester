package org.hypertrace.core.kafkastreams.framework;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.hypertrace.core.kafkastreams.framework.listeners.LoggingStateListener;
import org.hypertrace.core.kafkastreams.framework.listeners.LoggingStateRestoreListener;
import org.hypertrace.core.kafkastreams.framework.util.ExceptionUtils;
import org.hypertrace.core.serviceframework.background.PlatformBackgroundJob;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;

/**
 * Abstract base class that all Kafka Streams based applications need to extend
 */
public abstract class KafkaStreamsApp implements PlatformBackgroundJob {

  public static final String CLEANUP_LOCAL_STATE = "cleanup.local.state";

  protected final KafkaStreams app;

  protected KafkaStreamsApp(Config jobConfig) {
    Properties streamsConfig = getStreamsConfig(jobConfig);
    getLogger().info(ConfigUtils.propertiesAsList(streamsConfig));

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder = buildTopology(streamsConfig, streamsBuilder);
    Topology topology = streamsBuilder.build();
    getLogger().info(topology.describe().toString());

    app = new KafkaStreams(topology, streamsConfig);

    // useful for resetting local state - during testing or any other scenarios where
    // state (rocksdb) needs to be reset
    if (streamsConfig.containsKey(CLEANUP_LOCAL_STATE)) {
      boolean cleanup = Boolean.parseBoolean((String) streamsConfig.get(CLEANUP_LOCAL_STATE));
      if (cleanup) {
        getLogger().info("=== Resetting local state ===");
        app.cleanUp();
      }
    }

    app.setStateListener(new LoggingStateListener(app));
    app.setGlobalStateRestoreListener(new LoggingStateRestoreListener());
    app.setUncaughtExceptionHandler((thread, exception) -> {
          getLogger().error("Thread=[{}] encountered=[{}]. Will exit.", thread.getName(),
              ExceptionUtils.getStackTrace(exception));
          System.exit(1);
        }
    );
  }

  @Override
  public void run() throws Exception {
    app.start();
  }

  @Override
  public void stop() {
    // Refer to https://issues.apache.org/jira/browse/KAFKA-4366 for why
    // a timeout is needed
    app.close(Duration.ofSeconds(30));
  }

  protected abstract StreamsBuilder buildTopology(Properties streamsConfig, StreamsBuilder streamsBuilder);

  protected abstract Properties getStreamsConfig(Config jobConfig);

  protected abstract Logger getLogger();
}
