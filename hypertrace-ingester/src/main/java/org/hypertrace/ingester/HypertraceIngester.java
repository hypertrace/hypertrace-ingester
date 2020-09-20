package org.hypertrace.ingester;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.rawspansgrouper.RawSpansGrouper;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.core.spannormalizer.SpanNormalizer;
import org.hypertrace.core.viewgenerator.service.MultiViewGeneratorLauncher;
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

  private Map<String, String> jobNameToKey = new HashMap<>();
  private Map<String, KafkaStreamsApp> jobNameToSubTopology = new HashMap<>();

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
      default:
        throw new RuntimeException(String.format("Invalid configured sub-topology : [%s]", name));
    }
    return kafkaStreamsApp;
  }

  @Override
  public StreamsBuilder buildTopology(Map<String, Object> properties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {

    List<String> subTopologiesNames = getSubTopologiesNames(properties);

    for (String subTopologyName : subTopologiesNames) {
      KafkaStreamsApp subTopology = getSubTopologyInstance(subTopologyName);
      jobNameToSubTopology.put(subTopologyName, subTopology);
      Config subTopologyJobConfig = getSubJobConfig(subTopologyName);
      Map<String, Object> flattenSubTopologyConfig = subTopology
          .getStreamsConfig(subTopologyJobConfig);
      flattenSubTopologyConfig.put(subTopology.getJobConfigKey(), subTopologyJobConfig);
      addProperties(properties, flattenSubTopologyConfig);
      streamsBuilder = subTopology
          .buildTopology(properties, streamsBuilder, inputStreams);
      jobNameToKey.put(subTopologyName, subTopology.getJobConfigKey());
    }

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
    Set<String> inputTopics = new HashSet();
    List<String> subTopologiesNames = getSubTopologiesNames(properties);
    for (String subTopologyName : subTopologiesNames) {
      List<String> subTopologyInputTopics = jobNameToSubTopology.get(subTopologyName).getInputTopics(properties);
      subTopologyInputTopics.forEach(inputTopics::add);
    }
    return inputTopics.stream().collect(Collectors.toList());
  }

  @Override
  public List<String> getOutputTopics(Map<String, Object> properties) {
    Set<String> outputTopics = new HashSet();
    List<String> subTopologiesNames = getSubTopologiesNames(properties);
    for (String subTopologyName : subTopologiesNames) {
      List<String> subTopologyInputTopics = jobNameToSubTopology.get(subTopologyName).getOutputTopics(properties);
      subTopologyInputTopics.forEach(outputTopics::add);
    }
    return outputTopics.stream().collect(Collectors.toList());
  }

  private List<String> getSubTopologiesNames(Map<String, Object> properties) {
    return getJobConfig(properties).getStringList("sub.topology.names");
  }

  private Config getSubJobConfig(String jobName) {
    return configClient.getConfig(jobName,
        ConfigUtils.getEnvironmentProperty(CLUSTER_NAME),
        ConfigUtils.getEnvironmentProperty(POD_NAME),
        ConfigUtils.getEnvironmentProperty(CONTAINER_NAME)
    );
  }

  private Config getJobConfig(Map<String, Object> properties) {
    return (Config) properties.get(this.getJobConfigKey());
  }

  private void addProperties(Map<String, Object> baseProps, Map<String, Object> props) {
    props.forEach((k, v) -> {
      if (!baseProps.containsKey(k)) {
        baseProps.put(k, v);
      }
    });
  }
}
