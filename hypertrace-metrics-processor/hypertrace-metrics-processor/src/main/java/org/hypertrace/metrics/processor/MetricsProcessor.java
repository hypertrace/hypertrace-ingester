package org.hypertrace.metrics.processor;

import com.typesafe.config.Config;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsProcessor extends KafkaStreamsApp {
  private static final Logger logger = LoggerFactory.getLogger(MetricsProcessor.class);
  public static final String INPUT_TOPIC_CONFIG_KEY = "input.topic";
  public static final String OUTPUT_TOPIC_CONFIG_KEY = "output.topic";
  private static final String METRICS_PROCESSOR_JOB_CONFIG = "metrics-processor-job-config";

  public MetricsProcessor(ConfigClient configClient) {
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

    // input stream
    KStream<byte[], ResourceMetrics> inputStream =
        (KStream<byte[], ResourceMetrics>) inputStreams.get(inputTopic);
    if (inputStream == null) {
      inputStream =
          streamsBuilder.stream(
              inputTopic, Consumed.with(Serdes.ByteArray(), new OtlpMetricsSerde()));
      inputStreams.put(inputTopic, inputStream);
    }

    inputStream
        .transform(MetricsNormalizer::new)
        .transform(MetricsEnricher::new)
        .to(outputTopic, Produced.with(Serdes.ByteArray(), new OtlpMetricsSerde()));

    return streamsBuilder;
  }

  @Override
  public String getJobConfigKey() {
    return METRICS_PROCESSOR_JOB_CONFIG;
  }

  @Override
  public Logger getLogger() {
    return logger;
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
