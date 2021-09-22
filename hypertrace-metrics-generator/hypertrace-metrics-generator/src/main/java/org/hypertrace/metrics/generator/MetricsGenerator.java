package org.hypertrace.metrics.generator;

import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.viewgenerator.api.RawServiceView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsGenerator extends KafkaStreamsApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsGenerator.class);
  private static final String INPUT_TOPIC_CONFIG_KEY = "input.topic";
  private static final String OUTPUT_TOPIC_CONFIG_KEY = "output.topic";
  private static final String METRICS_GENERATOR_JOB_CONFIG = "metrics-generator-job-config";

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

    inputStream
        .transform(MetricsExtractor::new)
        .to(outputTopic, Produced.with(Serdes.ByteArray(), new OtelMetricsSerde()));

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
}
