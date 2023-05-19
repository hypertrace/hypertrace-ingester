package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_PRODUCER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_STATE_STORE_NAME;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_STATE_STORE;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.BYPASS_OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.OUTPUT_TOPIC_RAW_LOGS_CONFIG_KEY;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.partitioner.GroupPartitionerBuilder;
import org.hypertrace.core.kafkastreams.framework.partitioner.KeyHashPartitioner;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.spannormalizer.SpanIdentity;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.TraceState;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanPreProcessor;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanSerde;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanToAvroRawSpanTransformer;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanToLogRecordsTransformer;
import org.hypertrace.core.spannormalizer.jaeger.PreProcessedSpan;
import org.hypertrace.core.spannormalizer.rawspan.ByPassPredicate;
import org.hypertrace.core.spannormalizer.rawspan.RawSpanToStructuredTraceTransformer;
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
    String bypassOutputTopic = jobConfig.getString(BYPASS_OUTPUT_TOPIC_CONFIG_KEY);
    String outputTopicRawLogs = jobConfig.getString(OUTPUT_TOPIC_RAW_LOGS_CONFIG_KEY);

    KStream<byte[], JaegerSpanInternalModel.Span> inputStream =
        (KStream<byte[], JaegerSpanInternalModel.Span>) inputStreams.get(inputTopic);
    if (inputStream == null) {
      inputStream =
          streamsBuilder.stream(
              inputTopic, Consumed.with(Serdes.ByteArray(), new JaegerSpanSerde()));
      inputStreams.put(inputTopic, inputStream);
    }

    // Retrieve the default value serde defined in config and use it
    Serde valueSerde = defaultValueSerde(properties);
    Serde keySerde = defaultKeySerde(properties);

    StoreBuilder<KeyValueStore<TraceIdentity, TraceState>> traceStateStoreBuilder =
        Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(TRACE_STATE_STORE), keySerde, valueSerde)
            .withCachingEnabled();

    StoreBuilder<KeyValueStore<SpanIdentity, RawSpan>> spanStoreBuilder =
        Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(SPAN_STATE_STORE_NAME), keySerde, valueSerde)
            .withCachingEnabled();

    streamsBuilder.addStateStore(spanStoreBuilder);
    streamsBuilder.addStateStore(traceStateStoreBuilder);

    KStream<byte[], PreProcessedSpan> preProcessedStream =
        inputStream.transform(() -> new JaegerSpanPreProcessor(getGrpcChannelRegistry()));

    // logs output
    preProcessedStream.transform(JaegerSpanToLogRecordsTransformer::new).to(outputTopicRawLogs);

    KStream<TraceIdentity, RawSpan>[] branches =
        preProcessedStream
            .transform(JaegerSpanToAvroRawSpanTransformer::new)
            .branch(new ByPassPredicate(jobConfig), (key, value) -> true);

    KStream<TraceIdentity, RawSpan> bypassTopicBranch = branches[0];
    KStream<TraceIdentity, RawSpan> outputTopicBranch = branches[1];

    StreamPartitioner<String, StructuredTrace> tenantIsolationPartitionerForBypassTopic =
        new GroupPartitionerBuilder<String, StructuredTrace>()
            .buildPartitioner(
                "spans",
                jobConfig,
                (traceid, span) -> traceid,
                new KeyHashPartitioner<>(),
                getGrpcChannelRegistry());

    bypassTopicBranch
        .transform(RawSpanToStructuredTraceTransformer::new)
        .to(bypassOutputTopic, Produced.with(null, null, tenantIsolationPartitionerForBypassTopic));

    StreamPartitioner<TraceIdentity, StructuredTrace> tenantIsolationPartitionerForOutputTopic =
        new GroupPartitionerBuilder<TraceIdentity, StructuredTrace>()
            .buildPartitioner(
                "spans",
                jobConfig,
                (traceid, trace) -> traceid.getTenantId(),
                new KeyHashPartitioner<>(),
                getGrpcChannelRegistry());

    Produced<TraceIdentity, StructuredTrace> outputTopicProducer =
        Produced.with(null, null, tenantIsolationPartitionerForOutputTopic);
    outputTopicProducer = outputTopicProducer.withName(OUTPUT_TOPIC_PRODUCER);

    outputTopicBranch
        .transform(
            RawSpansProcessor::new,
            Named.as(RawSpansProcessor.class.getSimpleName()),
            SPAN_STATE_STORE_NAME,
            TRACE_STATE_STORE)
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
