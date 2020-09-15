package org.hypertrace.ingester;

import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.rawspansgrouper.RawSpansGrouper;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.core.spannormalizer.SpanNormalizer;
import org.hypertrace.traceenricher.trace.enricher.TraceEnricher;
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
  private static final String HYPERTRACE_INGESTER_JOB_CONFIG = "hypertrace-ingester-job-config";

  public HypertraceIngester(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(Map<String, Object> streamsProperties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {

    // build sub-topology for span-normalizer
    SpanNormalizer spanNormalizer = new SpanNormalizer(ConfigClientFactory.getClient());
    Config spanNormalizerConfig = getJobConfig("span-normalizer");
    Map<String, Object> spanNormalizerConfigMap = spanNormalizer
        .getStreamsConfig(spanNormalizerConfig);
    spanNormalizerConfigMap.put(spanNormalizer.getJobConfigKey(), spanNormalizerConfig);
    addProperties(streamsProperties, spanNormalizerConfigMap);
    streamsBuilder = spanNormalizer
        .buildTopology(spanNormalizerConfigMap, streamsBuilder, inputStreams);

    // build sub-topology for raw-spans-grouper
    RawSpansGrouper rawSpansGrouper = new RawSpansGrouper(ConfigClientFactory.getClient());
    Config rawSpansGrouperConfig = getJobConfig("raw-spans-grouper");
    Map<String, Object> rawSpansGrouperConfigMap = rawSpansGrouper
        .getStreamsConfig(rawSpansGrouperConfig);
    rawSpansGrouperConfigMap.put(rawSpansGrouper.getJobConfigKey(), rawSpansGrouperConfig);
    addProperties(streamsProperties, rawSpansGrouperConfigMap);
    streamsBuilder = rawSpansGrouper
        .buildTopology(rawSpansGrouperConfigMap, streamsBuilder, inputStreams);

    // build sub-topology for hypertrace-trace-enricher
    TraceEnricher traceEnricher = new TraceEnricher(ConfigClientFactory.getClient());
    Config traceEnricherConfig = getJobConfig("hypertrace-trace-enricher");
    Map<String, Object> traceEnricherConfigMap = traceEnricher
        .getStreamsConfig(traceEnricherConfig);
    traceEnricherConfigMap.put(traceEnricher.getJobConfigKey(), traceEnricherConfig);
    addProperties(streamsProperties, traceEnricherConfigMap);
    streamsBuilder = traceEnricher.buildTopology(streamsProperties, streamsBuilder, inputStreams);
    return streamsBuilder;
  }

  @Override
  public Map<String, Object> getStreamsConfig(Config jobConfig) {
    Map<String, Object> streamsConfig = new HashMap<>(
        ConfigUtils.getFlatMapConfig(jobConfig, KAFKA_STREAMS_CONFIG_KEY));
    return streamsConfig;
  }

  @Override
  public String getJobConfigKey() {
    return HYPERTRACE_INGESTER_JOB_CONFIG;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  @Override
  public List<String> getInputTopics(Map<String, Object> properties) {
    return Arrays.asList(
        "jaeger-spans",
        "raw-spans-from-jaeger-spans",
        "structured-traces-from-raw-spans");
  }

  @Override
  public List<String> getOutputTopics(Map<String, Object> properties) {
    return Arrays.asList(
        "raw-spans-from-jaeger-spans",
        "structured-traces-from-raw-spans",
        "enriched-structured-traces");
  }

  private Config getJobConfig(String jobName) {
    return configClient.getConfig(jobName,
        ConfigUtils.getEnvironmentProperty(CLUSTER_NAME),
        ConfigUtils.getEnvironmentProperty(POD_NAME),
        ConfigUtils.getEnvironmentProperty(CONTAINER_NAME)
    );
  }

  private void addProperties(Map<String, Object> baseProps, Map<String, Object> props) {
    props.forEach((k, v) -> {
      if (!baseProps.containsKey(k)) {
        baseProps.put(k, v);
      }
    });
  }
}
