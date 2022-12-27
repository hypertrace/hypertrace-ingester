package org.hypertrace.core.spannormalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Log;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import org.hypertrace.core.datamodel.StructuredTrace;
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

    // pipe in one more span which doesn't match spanDropFilters
    Span span2 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("2".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-2".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.method")
                    .setVStr("GET")
                    .build())
            .build();

    inputTopic.pipeInput(span2);
    KeyValue<TraceIdentity, RawSpan> kv1 = outputTopic.readKeyValue();
    assertNotNull(kv1);
    assertEquals("__default", kv1.key.getTenantId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-2".getBytes()).toByteArray()),
        HexUtils.getHex(kv1.key.getTraceId().array()));

    // pipe in one more span which match one of spanDropFilters (http.method & http.url)
    Span span3 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("3".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-3".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.method")
                    .setVStr("GET")
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.url")
                    .setVStr("http://xyz.com/health/check")
                    .build())
            .build();

    inputTopic.pipeInput(span3);
    assertTrue(outputTopic.isEmpty());

    // pipe in one more span which match one of spanDropFilters (grpc.url)
    Span span4 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("3".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-3".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("grpc.url")
                    .setVStr("doesn't match with input filter set")
                    .build())
            .build();

    inputTopic.pipeInput(span4);
    assertTrue(outputTopic.isEmpty());

    // pipe in one more span which match one of spanDropFilters (operation_name, and span.kind not
    // exists)
    Span span5 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("4".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-4".getBytes()))
            .setOperationName("/api/")
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("grpc1.url1")
                    .setVStr("xyz")
                    .build())
            .build();

    inputTopic.pipeInput(span5);
    assertTrue(outputTopic.isEmpty());

    // pipe in one more span which match one of spanDropFilters (operation_name, and span.kind not
    // exists)
    Span span6 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("6".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-6".getBytes()))
            .setOperationName("/api-should-be-there/")
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("grapc.url1")
                    .setVStr("xyz")
                    .build())
            .build();

    inputTopic.pipeInput(span6);
    KeyValue<TraceIdentity, RawSpan> span6KV = outputTopic.readKeyValue();
    assertEquals("__default", kv1.key.getTenantId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-6".getBytes()).toByteArray()),
        HexUtils.getHex(span6KV.key.getTraceId().array()));

    // pipe in one more span which does not match one of spanDropFilters (operation_name, and span.kind not
    // exists)
    Span span7 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("7".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-7".getBytes()))
            .setOperationName("/api/")
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("span.kind")
                    .setVStr("client")
                    .build())
            .build();

    inputTopic.pipeInput(span7);
    KeyValue<TraceIdentity, RawSpan> span7KV = outputTopic.readKeyValue();
    assertEquals("__default", kv1.key.getTenantId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-7".getBytes()).toByteArray()),
        HexUtils.getHex(span7KV.key.getTraceId().array()));
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "span-normalizer")
  public void whenByPassedExpectStructuredTraceToBeOutput() {
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

    Serde<StructuredTrace> structuredTraceSerde = new AvroSerde<>();
    structuredTraceSerde.configure(Map.of(), false);

    Serde<TraceIdentity> spanIdentitySerde = new AvroSerde<>();
    spanIdentitySerde.configure(Map.of(), true);

    TestOutputTopic outputTopic =
        td.createOutputTopic(
            config.getString(SpanNormalizerConstants.OUTPUT_TOPIC_CONFIG_KEY),
            spanIdentitySerde.deserializer(),
            rawSpanSerde.deserializer());

    TestOutputTopic bypassOutputTopic =
        td.createOutputTopic(
            config.getString(SpanNormalizerConstants.BYPASS_OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(),
            structuredTraceSerde.deserializer());

    TestOutputTopic rawLogOutputTopic =
        td.createOutputTopic(
            config.getString(SpanNormalizerConstants.OUTPUT_TOPIC_RAW_LOGS_CONFIG_KEY),
            spanIdentitySerde.deserializer(),
            new AvroSerde<>().deserializer());

    // with logs event, with bypass key
    // expects no output to raw-span-grouper
    // expects output to trace-enricher
    // expects log output
    Span span1 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("1".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("test.bypass")
                    .setVStr("true")
                    .build())
            .addLogs(
                Log.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(10).build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("z2")
                            .setVStr("some event detail")
                            .build()))
            .build();
    inputTopic.pipeInput(span1);

    // validate output for trace-enricher
    assertFalse(bypassOutputTopic.isEmpty());
    KeyValue<String, StructuredTrace> kv1 = bypassOutputTopic.readKeyValue();
    assertEquals("__default", kv1.value.getCustomerId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-1".getBytes()).toByteArray()),
        HexUtils.getHex(kv1.value.getTraceId().array()));

    // validate no output for raw-spans-grouper
    assertTrue(outputTopic.isEmpty());

    // validate that no change in log traffic
    assertFalse(rawLogOutputTopic.isEmpty());
    LogEvents logEvents = (LogEvents) rawLogOutputTopic.readKeyValue().value;
    Assertions.assertEquals(1, logEvents.getLogEvents().size());

    // with logs event, without bypass key
    // expects output to raw-span-grouper
    // expects no output to trace-enricher
    // expects log output
    Span span2 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("2".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-2".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.method")
                    .setVStr("GET")
                    .build())
            .addLogs(
                Log.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(10).build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("z2")
                            .setVStr("some event detail")
                            .build()))
            .build();

    inputTopic.pipeInput(span2);

    // validate that no output to trace-enricher
    assertTrue(bypassOutputTopic.isEmpty());

    // validate that output to raw-spans-grouper
    assertFalse(outputTopic.isEmpty());
    KeyValue<TraceIdentity, RawSpan> kv2 = outputTopic.readKeyValue();
    assertEquals("__default", kv2.key.getTenantId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-2".getBytes()).toByteArray()),
        HexUtils.getHex(kv2.key.getTraceId().array()));

    // validate that no change in log traffic
    assertFalse(rawLogOutputTopic.isEmpty());
    logEvents = (LogEvents) rawLogOutputTopic.readKeyValue().value;
    Assertions.assertEquals(1, logEvents.getLogEvents().size());

    // with logs event, with bypass key but false value
    // expects output to raw-span-grouper
    // expects no output to trace-enricher
    // expects log output
    Span span3 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("3".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-3".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.method")
                    .setVStr("GET")
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("test.bypass")
                    .setVStr("false")
                    .build())
            .addLogs(
                Log.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(10).build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("z2")
                            .setVStr("some event detail")
                            .build()))
            .build();

    inputTopic.pipeInput(span3);

    // validate that no output to trace-enricher
    assertTrue(bypassOutputTopic.isEmpty());

    // validate that output to raw-spans-grouper
    assertFalse(outputTopic.isEmpty());
    KeyValue<TraceIdentity, RawSpan> kv3 = outputTopic.readKeyValue();
    assertEquals("__default", kv3.key.getTenantId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-3".getBytes()).toByteArray()),
        HexUtils.getHex(kv3.key.getTraceId().array()));

    // validate that no change in log traffic
    assertFalse(rawLogOutputTopic.isEmpty());
    logEvents = (LogEvents) rawLogOutputTopic.readKeyValue().value;
    Assertions.assertEquals(1, logEvents.getLogEvents().size());
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "span-normalizer")
  public void testLaterArrivalJaegerSpans() {
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

    // case 1: within threshold, expect output
    Instant instant = Instant.now();
    Span span =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("1".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
            .setStartTime(Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
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

    // outside threshold, except no output to RawSpan
    Instant instant1 = Instant.now().minus(25, ChronoUnit.HOURS);
    Span span2 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("2".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-2".getBytes()))
            .setStartTime(Timestamp.newBuilder().setSeconds(instant1.getEpochSecond()).build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.method")
                    .setVStr("GET")
                    .build())
            .build();

    inputTopic.pipeInput(span2);
    Assertions.assertTrue(outputTopic.isEmpty());
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "span-normalizer")
  public void testTagsFilteringForJaegerSpans() {
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

    // makes sure that e2e works, so it tests basic scenario, rest of the
    // scenarios are covered in unit test of tagFilter
    // so configure for http extension attributes
    Instant instant = Instant.now();
    Span span =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("1".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
            .setStartTime(Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.request.header.x-allowed-1")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.response.header.x-allowed-2")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.request.header.x-not-allowed-1")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.response.header.x-not-allowed-2")
                    .setVStr(SERVICE_NAME)
                    .build())
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

    // test of attributes
    Assertions.assertEquals(3, value.getEvent().getAttributes().getAttributeMap().size());

    Assertions.assertTrue(
        value
            .getEvent()
            .getAttributes()
            .getAttributeMap()
            .containsKey("http.request.header.x-allowed-1"));
    Assertions.assertTrue(
        value
            .getEvent()
            .getAttributes()
            .getAttributeMap()
            .containsKey("http.response.header.x-allowed-2"));

    Assertions.assertFalse(
        value
            .getEvent()
            .getAttributes()
            .getAttributeMap()
            .containsKey("http.request.header.x-not-allowed-1"));
    Assertions.assertFalse(
        value
            .getEvent()
            .getAttributes()
            .getAttributeMap()
            .containsKey("http.response.header.x-not-allowed-2"));
  }
}
