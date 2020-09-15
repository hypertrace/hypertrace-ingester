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

      // get the lists of all input and output topics to pre create if any
      if (getAppConfig().hasPath(PRE_CREATE_TOPICS) &&
          getAppConfig().getBoolean(PRE_CREATE_TOPICS)) {
        List<String> topics = Streams.concat(
            getInputTopics(mergedProperties).stream(),
            getOutputTopics(mergedProperties).stream()
        ).collect(Collectors.toList());

        KafkaTopicCreator.createTopics((String) mergedProperties.getOrDefault(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ""),
            topics);
      }

      Map<String, KStream<?, ?>> sourceStreams = new HashMap<>();
      StreamsBuilder streamsBuilder = new StreamsBuilder();
      streamsBuilder = buildTopology(mergedProperties, streamsBuilder, sourceStreams);
      Topology topology = streamsBuilder.build();
      getLogger().info(topology.describe().toString());

      Properties properties = new Properties();
      properties.putAll(mergedProperties);
      getLogger().info(ConfigUtils.propertiesAsList(properties));

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

    baseStreamsConfig.put(getJobConfigKey(), getAppConfig());
    return baseStreamsConfig;
  }

  public abstract StreamsBuilder buildTopology(Map<String, Object> streamsConfig,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> sourceStreams);

  public abstract Map<String, Object> getStreamsConfig(Config jobConfig);

  public abstract String getJobConfigKey();

  public abstract Logger getLogger();

  public abstract List<String> getInputTopics(Map<String, Object> properties);

  public abstract List<String> getOutputTopics(Map<String, Object> properties);

  /**
   * Merge the props into baseProps
   */
  private Map<String, Object> mergeProperties(Map<String, Object> baseProps,
      Map<String, Object> props) {
    props.forEach(baseProps::put);
    return baseProps;
  }
}
