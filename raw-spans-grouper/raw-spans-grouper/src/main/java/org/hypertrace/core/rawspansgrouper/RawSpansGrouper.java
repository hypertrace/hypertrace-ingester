package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPANS_CHUNK_STATE_STORE_NAME;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_PRODUCER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_STATE_STORE_NAME;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_STATE_STORE;

import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.BytesSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.spannormalizer.RawSpansChunk;
import org.hypertrace.core.spannormalizer.RawSpansChunkIdentity;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.TraceStateV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawSpansGrouper extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(RawSpansGrouper.class);

  public RawSpansGrouper(ConfigClient configClient) {
    super(configClient);
  }

  public StreamsBuilder buildTopology(
      Map<String, Object> properties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {
    Config jobConfig = getJobConfig(properties);
    String inputTopic = jobConfig.getString(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY);

    KStream<TraceIdentity, RawSpan> inputStream =
        (KStream<TraceIdentity, RawSpan>) inputStreams.get(inputTopic);
    Serde valueSerde = defaultValueSerde(properties);
    Serde keySerde = defaultKeySerde(properties);

    if (inputStream == null) {
      inputStream =
          streamsBuilder
              // read the input topic
              .stream(inputTopic, Consumed.with(keySerde, valueSerde));
      inputStreams.put(inputTopic, inputStream);
    }

    // Retrieve the default value serde defined in config and use it

    StoreBuilder<KeyValueStore<TraceIdentity, TraceStateV2>> traceStateStoreBuilder =
        Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(TRACE_STATE_STORE), keySerde, valueSerde)
            .withCachingEnabled();

    StoreBuilder<KeyValueStore<RawSpansChunkIdentity, RawSpansChunk>> spanStoreBuilder =
        Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(SPANS_CHUNK_STATE_STORE_NAME), keySerde, valueSerde)
            .withCachingEnabled();

    streamsBuilder.addStateStore(spanStoreBuilder);
    streamsBuilder.addStateStore(traceStateStoreBuilder);

    Produced<String, StructuredTrace> outputTopicProducer = Produced.with(Serdes.String(), null);
    outputTopicProducer = outputTopicProducer.withName(OUTPUT_TOPIC_PRODUCER);

    inputStream
        .transform(
            RawSpansProcessor::new,
            Named.as(RawSpansProcessor.class.getSimpleName()),
            SPANS_CHUNK_STATE_STORE_NAME,
            TRACE_STATE_STORE)
        .to(outputTopic, outputTopicProducer);

    System.out.println(streamsBuilder.build().toString());
    return streamsBuilder;
  }

  @Override
  public String getJobConfigKey() {
    return RAW_SPANS_GROUPER_JOB_CONFIG;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  @Override
  public List<String> getInputTopics(Map<String, Object> properties) {
    return List.of(getJobConfig(properties).getString(INPUT_TOPIC_CONFIG_KEY));
  }

  @Override
  public List<String> getOutputTopics(Map<String, Object> properties) {
    return List.of(getJobConfig(properties).getString(OUTPUT_TOPIC_CONFIG_KEY));
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
