package org.hypertrace.core.kafkastreams.framework;


import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TOPOLOGY_OPTIMIZATION;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;

import com.google.common.collect.Streams;
import com.typesafe.config.Config;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
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
import org.slf4j.LoggerFactory;

public abstract class KafkaStreamsApp extends PlatformService {

  public static final String CLEANUP_LOCAL_STATE = "cleanup.local.state";
  public static final String PRE_CREATE_TOPICS = "precreate.topics";
  public static final String KAFKA_STREAMS_CONFIG_KEY = "kafka.streams.config";
  private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApp.class);
  protected KafkaStreams app;

  public KafkaStreamsApp(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    try {
      // configure properties
      Map<String, Object> baseStreamsConfig = getBaseStreamsConfig();
      Map<String, Object> streamsConfig = getJobStreamsConfig(getAppConfig());
      Map<String, Object> mergedProperties = mergeProperties(baseStreamsConfig, streamsConfig);

      // build topologies
      Map<String, KStream<?, ?>> sourceStreams = new HashMap<>();
      StreamsBuilder streamsBuilder = new StreamsBuilder();
      streamsBuilder = buildTopology(mergedProperties, streamsBuilder, sourceStreams);
      Topology topology = streamsBuilder.build();
      getLogger().info(topology.describe().toString());

      // finalized properties
      Properties properties = new Properties();
      properties.putAll(mergedProperties);
      getLogger().info(ConfigUtils.propertiesAsList(properties));

      // pre-create input/output topics required for kstream application
      preCreateTopics(mergedProperties);

      // create kstream app
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

    // Default streams configurations
    baseStreamsConfig.put(TOPOLOGY_OPTIMIZATION, "all");
    baseStreamsConfig.put(METRICS_RECORDING_LEVEL_CONFIG, "INFO");
    baseStreamsConfig
        .put(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, UseWallclockTimeOnInvalidTimestamp.class);
    baseStreamsConfig.put(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);

    // Default serde configurations
    baseStreamsConfig.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    baseStreamsConfig.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

    // Default producer configurations
    baseStreamsConfig.put(producerPrefix(LINGER_MS_CONFIG), "5000");
    baseStreamsConfig.put(producerPrefix(MAX_REQUEST_SIZE_CONFIG), "1048576");
    baseStreamsConfig.put(producerPrefix(COMPRESSION_TYPE_CONFIG), CompressionType.GZIP.name);

    // Default consumer configurations
    baseStreamsConfig.put(consumerPrefix(MAX_POLL_RECORDS_CONFIG), "1000");
    baseStreamsConfig.put(consumerPrefix(AUTO_OFFSET_RESET_CONFIG), "latest");
    baseStreamsConfig.put(consumerPrefix(AUTO_COMMIT_INTERVAL_MS_CONFIG), "5000");

    return baseStreamsConfig;
  }

  public abstract StreamsBuilder buildTopology(Map<String, Object> streamsConfig,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> sourceStreams);

  public Map<String, Object> getStreamsConfig(Config jobConfig) {
    Map<String, Object> streamsConfig = new HashMap<>(
        ConfigUtils.getFlatMapConfig(jobConfig, getStreamsConfigKey()));
    return streamsConfig;
  }

  public String getStreamsConfigKey() {
    return KAFKA_STREAMS_CONFIG_KEY;
  }

  public String getJobConfigKey() {
    return String.format("%s-job-config", getServiceName());
  }

  public Logger getLogger() {
    return logger;
  }

  public List<String> getInputTopics(Map<String, Object> properties) {
    return new ArrayList<>();
  }

  public List<String> getOutputTopics(Map<String, Object> properties) {
    return new ArrayList<>();
  }

  private Map<String, Object> getJobStreamsConfig(Config jobConfig) {
    Map<String, Object> properties = getStreamsConfig(jobConfig);
    if (!properties.containsKey(getJobConfigKey())) {
      properties.put(getJobConfigKey(), jobConfig);
    }
    return properties;
  }

  /**
   * Merge the props into baseProps
   */
  private Map<String, Object> mergeProperties(Map<String, Object> baseProps,
      Map<String, Object> props) {
    props.forEach(baseProps::put);
    return baseProps;
  }

  private void preCreateTopics(Map<String, Object> properties) {
    Config jobConfig = (Config) properties.get(getJobConfigKey());
    if (jobConfig.hasPath(PRE_CREATE_TOPICS) && jobConfig.getBoolean(PRE_CREATE_TOPICS)) {
      List<String> topics = Streams.concat(
          getInputTopics(properties).stream(),
          getOutputTopics(properties).stream()
      ).collect(Collectors.toList());

      KafkaTopicCreator
          .createTopics((String) properties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
              topics
          );
    }
  }
}
