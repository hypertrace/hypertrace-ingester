package org.hypertrace.core.spannormalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Log;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.datamodel.LogEvents;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanSerde;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class SpanNormalizerTest {

  private static final String SERVICE_NAME = "servicename";
  private SpanNormalizer underTest;

  @BeforeEach
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "span-normalizer")
  public void setUp() {
    underTest = new SpanNormalizer(ConfigClientFactory.getClient());
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "span-normalizer")
  public void whenJaegerSpansAreProcessedExpectRawSpansToBeOutput() {
    Config config =
        ConfigFactory.parseURL(
            getClass().getClassLoader().getResource("configs/span-normalizer/application.conf"));

    Map<String, Object> mergedProps = new HashMap<>();
    underTest.getBaseStreamsConfig().forEach(mergedProps::put);
    underTest.getStreamsConfig(config).forEach(mergedProps::put);
    mergedProps.put(SpanNormalizerConstants.SPAN_NORMALIZER_JOB_CONFIG, config);

    StreamsBuilder streamsBuilder =
        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Properties props = new Properties();
    mergedProps.forEach(props::put);

    TopologyTestDriver td = new TopologyTestDriver(streamsBuilder.build(), props);
    TestInputTopic<byte[], Span> inputTopic =
        td.createInputTopic(
            config.getString(SpanNormalizerConstants.INPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().serializer(),
            new JaegerSpanSerde().serializer());

    Serde<RawSpan> rawSpanSerde = new AvroSerde<>();
    rawSpanSerde.configure(Map.of(), false);

    Serde<TraceIdentity> spanIdentitySerde = new AvroSerde<>();
    spanIdentitySerde.configure(Map.of(), true);

    TestOutputTopic outputTopic =
        td.createOutputTopic(
            config.getString(SpanNormalizerConstants.OUTPUT_TOPIC_CONFIG_KEY),
            spanIdentitySerde.deserializer(),
            rawSpanSerde.deserializer());

    TestOutputTopic rawLogOutputTopic =
        td.createOutputTopic(
            config.getString(SpanNormalizerConstants.OUTPUT_TOPIC_RAW_LOGS_CONFIG_KEY),
            spanIdentitySerde.deserializer(),
            new AvroSerde<>().deserializer());

    Span span =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("1".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addLogs(
                Log.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(5).build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("e1")
                            .setVStr("some event detail")
                            .build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("e2")
                            .setVStr("some event detail")
                            .build()))
            .addLogs(
                Log.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(10).build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("z2")
                            .setVStr("some event detail")
                            .build()))
            .build();
    inputTopic.pipeInput(span);

    KeyValue<TraceIdentity, RawSpan> kv = outputTopic.readKeyValue();
    assertEquals("__default", kv.key.getTenantId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-1".getBytes()).toByteArray()),
        HexUtils.getHex(kv.key.getTraceId().array()));
    RawSpan value = kv.value;
    assertEquals(HexUtils.getHex("1".getBytes()), HexUtils.getHex((value).getEvent().getEventId()));
    assertEquals(SERVICE_NAME, value.getEvent().getServiceName());

    KeyValue<String, LogEvents> keyValue = rawLogOutputTopic.readKeyValue();
    LogEvents logEvents = keyValue.value;
    Assertions.assertEquals(2, logEvents.getLogEvents().size());
  }
}
