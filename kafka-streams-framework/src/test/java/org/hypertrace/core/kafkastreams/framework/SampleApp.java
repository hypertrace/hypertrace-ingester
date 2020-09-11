package org.hypertrace.core.kafkastreams.framework;


import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;

public class SampleApp extends KafkaStreamsApp {

  static final String APP_ID = "testapp";
  static String INPUT_TOPIC = "input";
  static String OUTPUT_TOPIC = "output";

  public SampleApp(ConfigClient configClient) {
    super(configClient);
  }

  static StreamsBuilder retainWordsLongerThan5Letters(StreamsBuilder streamsBuilder) {
    KStream<String, String> stream = streamsBuilder.stream(INPUT_TOPIC);
    stream.filter((k, v) -> v.length() > 5).to(OUTPUT_TOPIC);

    return streamsBuilder;
  }

  @Override
  public StreamsBuilder buildTopology(Map<String, Object> streamsConfig,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> sourceStreams) {
    return retainWordsLongerThan5Letters(streamsBuilder);
  }

  @Override
  public Map<String, Object> getStreamsConfig(Config jobConfig) {
    Map<String, Object> config = new HashMap<>();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    return config;
  }

  @Override
  public Logger getLogger() {
    return null;
  }

  @Override
  public List<String> getInputTopics() {
    return Arrays.asList(INPUT_TOPIC);
  }

  @Override
  public List<String> getOutputTopics() {
    return Arrays.asList(OUTPUT_TOPIC);
  }

  @Override
  public String getServiceName() {
    return "SampleApp";
  }
}
