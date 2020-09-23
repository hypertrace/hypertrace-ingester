package org.hypertrace.core.spannormalizer;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanSerde;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class SpanNormalizerTest {

  private SpanNormalizer underTest;

  @BeforeEach
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "span-normalizer")
  public void setUp() {
    underTest = new SpanNormalizer(ConfigClientFactory.getClient());
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "span-normalizer")
  public void whenJaegerSpansAreProcessedExpectRawSpansToBeOutput() {
    Config config = ConfigFactory.parseURL(
        getClass().getClassLoader().getResource("configs/span-normalizer/application.conf"));
    Map<?, ?> map = config.entrySet().stream().collect(Collectors.toMap(
        (Function<Entry<String, ConfigValue>, Object>) Entry::getKey,
        (Function<Entry<String, ConfigValue>, Object>) Entry::getValue));

    Map<String, Object> mergedProps = new HashMap<>();
    underTest.getBaseStreamsConfig().forEach(mergedProps::put);
    underTest.getStreamsConfig(config).forEach(mergedProps::put);
    mergedProps.put(SpanNormalizerConstants.SPAN_NORMALIZER_JOB_CONFIG, config);

    StreamsBuilder streamsBuilder = underTest
        .buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Properties props = new Properties();
    mergedProps.forEach(props::put);

    TopologyTestDriver td = new TopologyTestDriver(streamsBuilder.build(), props);
    TestInputTopic<byte[], Span> inputTopic = td
        .createInputTopic(config.getString(SpanNormalizerConstants.INPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().serializer(), new JaegerSpanSerde().serializer());

    Serde<RawSpan> rawSpanSerde = new AvroSerde<>();
    rawSpanSerde.configure(Map.of(), false);

    TestOutputTopic outputTopic = td
        .createOutputTopic(config.getString(SpanNormalizerConstants.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(), rawSpanSerde.deserializer());

    Span span = Span.newBuilder().setSpanId(ByteString.copyFrom("1".getBytes())).build();
    inputTopic.pipeInput(span);
    Object value = outputTopic.readValue();
    assertEquals(HexUtils.getHex("1".getBytes()),
        HexUtils.getHex(((RawSpan) value).getEvent().getEventId()));
  }
}