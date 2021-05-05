package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_PRODUCER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_WINDOW_STORE;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_WINDOW_STORE_RETENTION_TIME_MINS;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_WINDOW_STORE_SEGMENT_SIZE_MINS;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_EMIT_TRIGGER_STORE;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.spannormalizer.SpanIdentity;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.TraceState;
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
    if (inputStream == null) {
      inputStream =
          streamsBuilder
              // read the input topic
              .stream(inputTopic);
      inputStreams.put(inputTopic, inputStream);
    }

    // Retrieve the default value serde defined in config and use it
    Serde valueSerde = defaultValueSerde(properties);
    Serde keySerde = defaultKeySerde(properties);

    long spanWindowStoreRetentionTimeMins =
        getAppConfig().hasPath(SPAN_WINDOW_STORE_RETENTION_TIME_MINS)
            ? getAppConfig().getLong(SPAN_WINDOW_STORE_RETENTION_TIME_MINS)
            : 60;
    long spanWindowStoreSegmentSizeMins =
        getAppConfig().hasPath(SPAN_WINDOW_STORE_SEGMENT_SIZE_MINS)
            ? getAppConfig().getLong(SPAN_WINDOW_STORE_SEGMENT_SIZE_MINS)
            : 20;
    StoreBuilder<WindowStore<SpanIdentity, RawSpan>> spanWindowStoreBuilder =
        Stores.windowStoreBuilder(
            new RocksDbWindowBytesStoreSupplier(
                SPAN_WINDOW_STORE,
                // retention period of window
                // so data older than 1 hour will be cleaned up
                Duration.ofHours(spanWindowStoreRetentionTimeMins).toMillis(),
                // length of a segment in rocksdb, so if segment size is 5mins and retention is
                // 60mins
                // there will be 12 segments in rocksdb
                Duration.ofMinutes(spanWindowStoreSegmentSizeMins).toMillis(),
                // duration of a window,
                // this param doesn't play any role in actual persistence of data
                // and is more of a logical construct used while returning the data
                Duration.ofMinutes(1).toMillis(),
                false,
                false),
            keySerde,
            valueSerde);

    StoreBuilder<KeyValueStore<TraceIdentity, TraceState>> traceEmitTriggerStoreBuilder =
        Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(TRACE_EMIT_TRIGGER_STORE), keySerde, valueSerde)
            .withCachingEnabled();

    streamsBuilder.addStateStore(spanWindowStoreBuilder);
    streamsBuilder.addStateStore(traceEmitTriggerStoreBuilder);

    Produced<String, StructuredTrace> outputTopicProducer = Produced.with(Serdes.String(), null);
    outputTopicProducer = outputTopicProducer.withName(OUTPUT_TOPIC_PRODUCER);

    inputStream
        .transform(
            RawSpansProcessor::new,
            Named.as(RawSpansProcessor.class.getSimpleName()),
            SPAN_WINDOW_STORE,
            TRACE_EMIT_TRIGGER_STORE)
        .to(outputTopic, outputTopicProducer);

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
