package org.hypertrace.core.kafkastreams.framework;


import static org.hypertrace.core.kafkastreams.framework.constants.KafkaStreamsAppConstants.JOB_CONFIG;

import com.google.common.collect.Streams;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.listeners.LoggingStateListener;
import org.hypertrace.core.kafkastreams.framework.listeners.LoggingStateRestoreListener;
import org.hypertrace.core.kafkastreams.framework.timestampextractors.UseWallclockTimeOnInvalidTimestamp;
import org.hypertrace.core.kafkastreams.framework.topics.creator.KafkaTopicCreator;
import org.hypertrace.core.kafkastreams.framework.util.ExceptionUtils;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;

public abstract class KafkaStreamsApp extends PlatformService {

  public static final String CLEANUP_LOCAL_STATE = "cleanup.local.state";
  public static final String PRE_CREATE_TOPICS = "precreate.topics";

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

      // get the lists of all input and output topics to pre create if any
      if (getAppConfig().hasPath(PRE_CREATE_TOPICS) &&
          getAppConfig().getBoolean(PRE_CREATE_TOPICS)) {
        List<String> topics = Streams.concat(
            getInputTopics().stream(),
            getOutputTopics().stream()
        ).collect(Collectors.toList());

        KafkaTopicCreator.createTopics(properties.getProperty(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ""),
            topics);
      }

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
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> sourceStreams);

  public abstract Map<String, Object> getStreamsConfig(Config jobConfig);

  public abstract Logger getLogger();

  public abstract List<String> getInputTopics();

  public abstract List<String> getOutputTopics();

  /**
   * Merge the props into baseProps
   */
  private Map<String, Object> mergeProperties(Map<String, Object> baseProps,
      Map<String, Object> props) {
    props.forEach(baseProps::put);
    return baseProps;
  }
}
