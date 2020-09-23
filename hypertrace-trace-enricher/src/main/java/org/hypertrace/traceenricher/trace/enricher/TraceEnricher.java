package org.hypertrace.traceenricher.trace.enricher;

import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.STRUCTURED_TRACES_ENRICHMENT_JOB_CONFIG_KEY;

import com.typesafe.config.Config;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceEnricher extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(TraceEnricher.class);

  public TraceEnricher(ConfigClient configClient) {
    super(configClient);
  }

  @Override

  public StreamsBuilder buildTopology(Map<String, Object> streamsProperties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {

    Config jobConfig = getJobConfig(streamsProperties);
    String inputTopic = jobConfig.getString(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY);

    KStream<String, StructuredTrace> inputStream = (KStream<String, StructuredTrace>) inputStreams
        .get(inputTopic);
    if (inputStream == null) {
      inputStream = streamsBuilder
          .stream(inputTopic, Consumed.with(Serdes.String(), null));
      inputStreams.put(inputTopic, inputStream);
    }

    inputStream
        .transform(StructuredTraceEnrichProcessor::new)
        .to(outputTopic, Produced.keySerde(Serdes.String()));

    return streamsBuilder;
  }

  @Override
  public String getJobConfigKey() {
    return STRUCTURED_TRACES_ENRICHMENT_JOB_CONFIG_KEY;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  @Override
  public List<String> getInputTopics(Map<String, Object> properties) {
    Config jobConfig = getJobConfig(properties);
    return Collections.singletonList(jobConfig.getString(INPUT_TOPIC_CONFIG_KEY));
  }

  @Override
  public List<String> getOutputTopics(Map<String, Object> properties) {
    Config jobConfig = getJobConfig(properties);
    return Collections.singletonList(jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY));
  }

  private Config getJobConfig(Map<String, Object> properties) {
    return (Config) properties.get(getJobConfigKey());
  }
}
