package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_PRODUCER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.RocksDbSessionBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.RawSpans;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawSpansGrouperPoC extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(RawSpansGrouper.class);

  public RawSpansGrouperPoC(ConfigClient configClient) {
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
//    Serde valueSerde = defaultValueSerde(properties);
//    StoreBuilder<KeyValueStore<TraceIdentity, ValueAndTimestamp<RawSpans>>> traceStoreBuilder =
//        Stores.sessionStoreBuilder(
//            new RocksDbSessionBytesStoreSupplier("", Duration.ofMinutes(5).toMillis()),
//            (Serde<TraceIdentity>) null,
//            new ValueAndTimestampSerde<>(valueSerde));
//
//
//    streamsBuilder.addStateStore(traceStoreBuilder);

    Produced<String, StructuredTrace> outputTopicProducer = Produced.with(Serdes.String(), null);
    outputTopicProducer = outputTopicProducer.withName(OUTPUT_TOPIC_PRODUCER);

    inputStream.groupByKey().windowedBy(SessionWindows.with(Duration.ofSeconds(30))).aggregate(
        () -> RawSpans.newBuilder().build(),
        (Aggregator<? super TraceIdentity, ? super RawSpan, RawSpans>) (k, v, vr) -> {
          vr.getRawSpans().add(v);
          return vr;
        },
        (k, v1, v2) -> {
          v1.getRawSpans().addAll(v2.getRawSpans());
          return v1;
        }
    ).mapValues((k, v) -> StructuredTraceBuilder.buildStructuredTraceFromRawSpans(
        v.getRawSpans(), k.key().getTraceId(), k.key().getTenantId(), null))
        .toStream((key, value) -> "").to(outputTopic, outputTopicProducer);
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
}
