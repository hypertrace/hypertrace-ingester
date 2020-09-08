package org.hypertrace.core.kafkastreams.framework;


import com.typesafe.config.Config;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;

public class SampleApp extends KafkaStreamsApp {

  static String INPUT_TOPIC = "input";
  static String OUTPUT_TOPIC = "output";
  static final String APP_ID = "testapp";

  protected SampleApp(Config jobConfig) {
    super(jobConfig);
  }

  @Override
  protected StreamsBuilder buildTopology(Properties streamsConfig, StreamsBuilder streamsBuilder) {
    return retainWordsLongerThan5Letters(streamsBuilder);
  }

  @Override
  protected Properties getStreamsConfig(Config jobConfig) {
    Properties config = new Properties();
    config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
    config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    return config;
  }

  @Override
  protected Logger getLogger() {
    return null;
  }

  static StreamsBuilder retainWordsLongerThan5Letters(StreamsBuilder streamsBuilder) {
    KStream<String, String> stream = streamsBuilder.stream(INPUT_TOPIC);
    stream.filter((k, v) -> v.length() > 5).to(OUTPUT_TOPIC);

    return streamsBuilder;
  }
}