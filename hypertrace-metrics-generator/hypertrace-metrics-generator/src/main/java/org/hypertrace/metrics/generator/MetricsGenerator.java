package org.hypertrace.metrics.generator;

import com.typesafe.config.Config;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.metrics.generator.api.v1.Metric;
import org.hypertrace.metrics.generator.api.v1.MetricIdentity;
import org.hypertrace.metrics.generator.api.v1.serde.MetricIdentitySerde;
import org.hypertrace.metrics.generator.api.v1.serde.MetricSerde;
import org.hypertrace.metrics.generator.api.v1.serde.OtlpMetricsSerde;
import org.hypertrace.viewgenerator.api.RawServiceView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsGenerator extends KafkaStreamsApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsGenerator.class);
  public static final String INPUT_TOPIC_CONFIG_KEY = "input.topic";
  public static final String OUTPUT_TOPIC_CONFIG_KEY = "output.topic";
  public static final String METRICS_GENERATOR_JOB_CONFIG = "metrics-generator-job-config";
  public static final String METRICS_IDENTITY_STORE = "metric-identity-store";
  public static final String METRICS_IDENTITY_VALUE_STORE = "metric-identity-value-Store";
  public static final String OUTPUT_TOPIC_PRODUCER = "output-topic-producer";

  public MetricsGenerator(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(
      Map<String, Object> streamsProperties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {

    Config jobConfig = getJobConfig(streamsProperties);
    String inputTopic = jobConfig.getString(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY);

    KStream<String, RawServiceView> inputStream =
        (KStream<String, RawServiceView>) inputStreams.get(inputTopic);
    if (inputStream == null) {
      inputStream = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), (Serde) null));
      inputStreams.put(inputTopic, inputStream);
    }

    StoreBuilder<KeyValueStore<MetricIdentity, Long>> metricIdentityStoreBuilder =
        Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(METRICS_IDENTITY_STORE),
                new MetricIdentitySerde(),
                new LongSerde())
            .withCachingEnabled();

    StoreBuilder<KeyValueStore<MetricIdentity, Metric>> metricIdentityToValueStoreBuilder =
        Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(METRICS_IDENTITY_VALUE_STORE),
                new MetricIdentitySerde(),
                new MetricSerde())
            .withCachingEnabled();

    streamsBuilder.addStateStore(metricIdentityStoreBuilder);
    streamsBuilder.addStateStore(metricIdentityToValueStoreBuilder);

    Produced<byte[], ResourceMetrics> outputTopicProducer =
        Produced.with(Serdes.ByteArray(), new OtlpMetricsSerde());
    outputTopicProducer = outputTopicProducer.withName(OUTPUT_TOPIC_PRODUCER);

    inputStream
        .transform(
            MetricsProcessor::new,
            Named.as(MetricsProcessor.class.getSimpleName()),
            METRICS_IDENTITY_STORE,
            METRICS_IDENTITY_VALUE_STORE)
        .to(outputTopic, outputTopicProducer);

    return streamsBuilder;
  }

  @Override
  public String getJobConfigKey() {
    return METRICS_GENERATOR_JOB_CONFIG;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  @Override
  public List<String> getInputTopics(Map<String, Object> properties) {
    Config jobConfig = getJobConfig(properties);
    return List.of(jobConfig.getString(INPUT_TOPIC_CONFIG_KEY));
  }

  @Override
  public List<String> getOutputTopics(Map<String, Object> properties) {
    Config jobConfig = getJobConfig(properties);
    return List.of(jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY));
  }

  private Config getJobConfig(Map<String, Object> properties) {
    return (Config) properties.get(getJobConfigKey());
  }

  private Serde defaultValueSerde(Map<String, Object> properties) {
    StreamsConfig config = new StreamsConfig(properties);
    return config.defaultValueSerde();
  }

  private Serde defaultKeySerde(Map<String, Object> properties) {
    StreamsConfig config = new StreamsConfig(properties);
    return config.defaultKeySerde();
  }
}
