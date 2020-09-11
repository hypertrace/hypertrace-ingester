package org.hypertrace.core.kafkastreams.framework;

import static org.hypertrace.core.kafkastreams.framework.constants.KafkaStreamsAppConstants.JOB_CONFIG;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.listeners.LoggingStateListener;
import org.hypertrace.core.kafkastreams.framework.listeners.LoggingStateRestoreListener;
import org.hypertrace.core.kafkastreams.framework.timestampextractors.UseWallclockTimeOnInvalidTimestamp;
import org.hypertrace.core.kafkastreams.framework.util.ExceptionUtils;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;

public abstract class KafkaStreamsApp extends PlatformService {

  public static final String CLEANUP_LOCAL_STATE = "cleanup.local.state";
  protected KafkaStreams app;

  public KafkaStreamsApp(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    try {
      Map<String, Object> baseStreamsConfig = getBaseStreamsConfig();
      Map<String, Object> streamsConfig = getStreamsConfig(getAppConfig());

      Map<String, Object> mergedProperties = mergeProperties(baseStreamsConfig, streamsConfig);

      Properties properties = new Properties();
      properties.putAll(mergedProperties);

      getLogger().info(ConfigUtils.propertiesAsList(properties));

      Map<String, KStream<?, ?>> sourceStreams = new HashMap<>();
      StreamsBuilder streamsBuilder = new StreamsBuilder();
      streamsBuilder = buildTopology(mergedProperties, streamsBuilder, sourceStreams);
      Topology topology = streamsBuilder.build();
      getLogger().info(topology.describe().toString());

      app = new KafkaStreams(topology, properties);

      // useful for resetting local state - during testing or any other scenarios where
      // state (rocksdb) needs to be reset
      if (properties.containsKey(CLEANUP_LOCAL_STATE)) {
        boolean cleanup = Boolean.parseBoolean((String) properties.get(CLEANUP_LOCAL_STATE));
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
    } catch (Exception e) {
      getLogger().error("Error initializing - ", e);
      e.printStackTrace();
      System.exit(1);
    }
  }

  @Override
  protected void doStart() {
    try {
      app.start();
    } catch (Exception e) {
      getLogger().error("Error starting - ", e);
      e.printStackTrace();
      System.exit(1);
    }
  }

  @Override
  protected void doStop() {
    app.close(Duration.ofSeconds(30));
  }

  @Override
  public boolean healthCheck() {
    return true;
  }

  /**
   * @return all common kafka-streams properties. Typically applications don't need to override this
   */
  public Map<String, Object> getBaseStreamsConfig() {
    Map<String, Object> baseStreamsConfig = new HashMap<>();
    baseStreamsConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        UseWallclockTimeOnInvalidTimestamp.class);
    baseStreamsConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);

    baseStreamsConfig.put(JOB_CONFIG, getAppConfig());
    return baseStreamsConfig;
  }

  public abstract StreamsBuilder buildTopology(Map<String, Object> streamsConfig,
      StreamsBuilder streamsBuilder, Map<String, KStream<?, ?>> sourceStreams);

  public abstract Map<String, Object> getStreamsConfig(Config jobConfig);

  public abstract Logger getLogger();

  /**
   * Merge the props into baseProps
   */
  private Map<String, Object> mergeProperties(Map<String, Object> baseProps,
      Map<String, Object> props) {
    props.forEach(baseProps::put);
    return baseProps;
  }
}
