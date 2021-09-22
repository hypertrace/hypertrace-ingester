package org.hypertrace.ingester;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.rawspansgrouper.RawSpansGrouper;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.core.spannormalizer.SpanNormalizer;
import org.hypertrace.core.viewgenerator.service.MultiViewGeneratorLauncher;
import org.hypertrace.metrics.generator.MetricsGenerator;
import org.hypertrace.metrics.processor.MetricsProcessor;
import org.hypertrace.traceenricher.trace.enricher.TraceEnricher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Hypertrace ingestion pipeline */
public class HypertraceIngester extends KafkaStreamsApp {

  private static final Logger LOGGER = LoggerFactory.getLogger(HypertraceIngester.class);

  private static final String HYPERTRACE_INGESTER_JOB_CONFIG = "hypertrace-ingester-job-config";

  private Map<String, Pair<String, KafkaStreamsApp>> jobNameToSubTopology = new HashMap<>();

  public HypertraceIngester(ConfigClient configClient) {
    super(configClient);
  }

  private KafkaStreamsApp getSubTopologyInstance(String name) {
    KafkaStreamsApp kafkaStreamsApp;
    switch (name) {
      case "span-normalizer":
        kafkaStreamsApp = new SpanNormalizer(ConfigClientFactory.getClient());
        break;
      case "raw-spans-grouper":
        kafkaStreamsApp = new RawSpansGrouper(ConfigClientFactory.getClient());
        break;
      case "hypertrace-trace-enricher":
        kafkaStreamsApp = new TraceEnricher(ConfigClientFactory.getClient());
        break;
      case "all-views":
        kafkaStreamsApp = new MultiViewGeneratorLauncher(ConfigClientFactory.getClient());
        break;
      case "hypertrace-metrics-generator":
        kafkaStreamsApp = new MetricsGenerator(ConfigClientFactory.getClient());
        break;
      case "hypertrace-metrics-processor":
        kafkaStreamsApp = new MetricsProcessor(ConfigClientFactory.getClient());
        break;
      default:
        throw new RuntimeException(String.format("Invalid configured sub-topology : [%s]", name));
    }
    return kafkaStreamsApp;
  }

  @Override
  public StreamsBuilder buildTopology(
      Map<String, Object> properties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {

    List<String> subTopologiesNames = getSubTopologiesNames(properties);

    for (String subTopologyName : subTopologiesNames) {
      LOGGER.info("Building sub topology :{}", subTopologyName);

      // create an instance and retains is reference to be used later in other methods
      KafkaStreamsApp subTopology = getSubTopologyInstance(subTopologyName);
      jobNameToSubTopology.put(
          subTopologyName, Pair.of(subTopology.getJobConfigKey(), subTopology));

      // need to use its own copy as input/output topics are different
      Config subTopologyJobConfig = getSubJobConfig(subTopologyName);
      Map<String, Object> flattenSubTopologyConfig =
          subTopology.getStreamsConfig(subTopologyJobConfig);
      flattenSubTopologyConfig.put(subTopology.getJobConfigKey(), subTopologyJobConfig);

      // add specific job properties
      addProperties(properties, flattenSubTopologyConfig);
      streamsBuilder = subTopology.buildTopology(properties, streamsBuilder, inputStreams);

      // retain per job key and its topology
      jobNameToSubTopology.put(
          subTopologyName, Pair.of(subTopology.getJobConfigKey(), subTopology));
    }

    return streamsBuilder;
  }

  @Override
  public String getJobConfigKey() {
    return HYPERTRACE_INGESTER_JOB_CONFIG;
  }

  @Override
  public List<String> getInputTopics(Map<String, Object> properties) {
    Set<String> inputTopics = new HashSet();
    for (Map.Entry<String, Pair<String, KafkaStreamsApp>> entry : jobNameToSubTopology.entrySet()) {
      List<String> subTopologyInputTopics = entry.getValue().getRight().getInputTopics(properties);
      subTopologyInputTopics.forEach(inputTopics::add);
    }
    return inputTopics.stream().collect(Collectors.toList());
  }

  @Override
  public List<String> getOutputTopics(Map<String, Object> properties) {
    Set<String> outputTopics = new HashSet();
    for (Map.Entry<String, Pair<String, KafkaStreamsApp>> entry : jobNameToSubTopology.entrySet()) {
      List<String> subTopologyInputTopics = entry.getValue().getRight().getOutputTopics(properties);
      subTopologyInputTopics.forEach(outputTopics::add);
    }
    return outputTopics.stream().collect(Collectors.toList());
  }

  private List<String> getSubTopologiesNames(Map<String, Object> properties) {
    return getJobConfig(properties).getStringList("sub.topology.names");
  }

  private Config getSubJobConfig(String jobName) {
    return configClient.getConfig(
        jobName,
        ConfigUtils.getEnvironmentProperty("cluster.name"),
        ConfigUtils.getEnvironmentProperty("pod.name"),
        ConfigUtils.getEnvironmentProperty("container.name"));
  }

  private Config getJobConfig(Map<String, Object> properties) {
    return (Config) properties.get(this.getJobConfigKey());
  }

  private void addProperties(Map<String, Object> baseProps, Map<String, Object> props) {
    props.forEach(
        (k, v) -> {
          if (!baseProps.containsKey(k)) {
            baseProps.put(k, v);
          }
        });
  }
}
