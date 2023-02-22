package org.hypertrace.core.spannormalizer;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.BYPASS_OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.OUTPUT_TOPIC_RAW_LOGS_CONFIG_KEY;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.SPAN_NORMALIZER_JOB_CONFIG;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.partitioner.GroupPartitionerBuilder;
import org.hypertrace.core.kafkastreams.framework.partitioner.KeyHashPartitioner;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanPreProcessor;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanSerde;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanToAvroRawSpanTransformer;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanToLogRecordsTransformer;
import org.hypertrace.core.spannormalizer.jaeger.PreProcessedSpan;
import org.hypertrace.core.spannormalizer.rawspan.ByPassPredicate;
import org.hypertrace.core.spannormalizer.rawspan.RawSpanToStructuredTraceTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpanNormalizer extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(SpanNormalizer.class);

  public SpanNormalizer(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(
      Map<String, Object> streamsProperties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {

    Config jobConfig = getJobConfig(streamsProperties);
    String inputTopic = jobConfig.getString(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY);
    String bypassOutputTopic = jobConfig.getString(BYPASS_OUTPUT_TOPIC_CONFIG_KEY);
    String outputTopicRawLogs = jobConfig.getString(OUTPUT_TOPIC_RAW_LOGS_CONFIG_KEY);

    KStream<byte[], Span> inputStream = (KStream<byte[], Span>) inputStreams.get(inputTopic);
    if (inputStream == null) {
      inputStream =
          streamsBuilder.stream(
              inputTopic, Consumed.with(Serdes.ByteArray(), new JaegerSpanSerde()));
      inputStreams.put(inputTopic, inputStream);
    }

    KStream<byte[], PreProcessedSpan> preProcessedStream =
        inputStream.transform(JaegerSpanPreProcessor::new);

    KStream<TraceIdentity, RawSpan>[] branches =
        preProcessedStream
            .transform(JaegerSpanToAvroRawSpanTransformer::new)
            .branch(new ByPassPredicate(jobConfig), (key, value) -> true);
    branches[0].transform(RawSpanToStructuredTraceTransformer::new).to(bypassOutputTopic);

    StreamPartitioner<TraceIdentity, RawSpan> groupPartitioner =
        new GroupPartitionerBuilder<TraceIdentity, RawSpan>()
            .buildPartitioner(
                "spans",
                jobConfig,
                (traceid, span) -> traceid.getTenantId(),
                new KeyHashPartitioner<>(),
                new GrpcChannelRegistry());
    branches[1].to(outputTopic, Produced.with(null, null, groupPartitioner));

    preProcessedStream.transform(JaegerSpanToLogRecordsTransformer::new).to(outputTopicRawLogs);
    return streamsBuilder;
  }

  @Override
  public String getJobConfigKey() {
    return SPAN_NORMALIZER_JOB_CONFIG;
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
    return List.of(
        jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY),
        jobConfig.getString(OUTPUT_TOPIC_RAW_LOGS_CONFIG_KEY));
  }

  private Config getJobConfig(Map<String, Object> properties) {
    return (Config) properties.get(getJobConfigKey());
  }
}
