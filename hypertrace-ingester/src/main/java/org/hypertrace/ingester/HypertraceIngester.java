package org.hypertrace.ingester;

import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.rawspansgrouper.RawSpansGrouper;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.core.spannormalizer.SpanNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hypertrace ingestion pipeline
 */
public class HypertraceIngester extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(HypertraceIngester.class);

  private static final String CLUSTER_NAME = "cluster.name";
  private static final String POD_NAME = "pod.name";
  private static final String CONTAINER_NAME = "container.name";
  private static final String KAFKA_STREAMS_CONFIG_KEY = "kafka.streams.config";

  public HypertraceIngester(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(Map<String, Object> streamsProperties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {

    // create span normalizer
    SpanNormalizer spanNormalizer = new SpanNormalizer(ConfigClientFactory.getClient());
    Config spanNormalizerConfig = getJobConfig("span-normalizer");
    Map<String, Object> spanNormalizerConfigMap = spanNormalizer
        .getStreamsConfig(spanNormalizerConfig);
    streamsBuilder = spanNormalizer
        .buildTopology(spanNormalizerConfigMap, streamsBuilder, inputStreams);

    // create raw spans grouper
    RawSpansGrouper rawSpansGrouper = new RawSpansGrouper(ConfigClientFactory.getClient());
    Config rawSpansGrouperConfig = getJobConfig("raw-spans-grouper");
    Map<String, Object> rawSpansGrouperConfigMap = rawSpansGrouper
        .getStreamsConfig(rawSpansGrouperConfig);
    streamsBuilder = rawSpansGrouper
        .buildTopology(rawSpansGrouperConfigMap, streamsBuilder, inputStreams);

    return streamsBuilder;
  }

  @Override
  public Map<String, Object> getStreamsConfig(Config jobConfig) {
    Map<String, Object> streamsConfig = new HashMap<>(
        ConfigUtils.getFlatMapConfig(jobConfig, KAFKA_STREAMS_CONFIG_KEY));
    return streamsConfig;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  @Override
  public List<String> getInputTopics() {
    return Arrays.asList("jaeger-spans", "raw-spans-from-jaeger-spans");
  }

  @Override
  public List<String> getOutputTopics() {
    return Arrays.asList("raw-spans-from-jaeger-spans", "structured-traces-from-raw-spans");
  }

  private Config getJobConfig(String jobName) {
    return configClient.getConfig(jobName,
        ConfigUtils.getEnvironmentProperty(CLUSTER_NAME),
        ConfigUtils.getEnvironmentProperty(POD_NAME),
        ConfigUtils.getEnvironmentProperty(CONTAINER_NAME)
    );
  }
}
