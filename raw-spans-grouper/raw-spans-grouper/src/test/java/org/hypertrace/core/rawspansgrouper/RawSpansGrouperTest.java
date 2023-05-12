package org.hypertrace.core.rawspansgrouper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.LogEvents;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanSerde;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class RawSpansGrouperTest {

  private static final String SERVICE_NAME = "servicename";

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "raw-spans-grouper")
  public void whenSpansMatchDropFilterThenExpectThemToBeDropped(@TempDir Path tempDir) {
    File file = tempDir.resolve("state").toFile();

    RawSpansGrouper underTest = new RawSpansGrouper(ConfigClientFactory.getClient());
    Config config =
        ConfigFactory.parseURL(
            getClass().getClassLoader().getResource("configs/raw-spans-grouper/application.conf"));

    Map<String, Object> baseProps = underTest.getBaseStreamsConfig();
    Map<String, Object> streamsProps = underTest.getStreamsConfig(config);
    baseProps.forEach(streamsProps::put);
    Map<String, Object> mergedProps = streamsProps;

    mergedProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    mergedProps.put(RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG, config);
    mergedProps.put(StreamsConfig.STATE_DIR_CONFIG, file.getAbsolutePath());

    StreamsBuilder streamsBuilder =
        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Properties props = new Properties();
    mergedProps.forEach(props::put);

    Serde defaultValueSerde = new StreamsConfig(mergedProps).defaultValueSerde();

    Serde<TraceIdentity> traceIdentitySerde = new StreamsConfig(mergedProps).defaultKeySerde();

    TopologyTestDriver td = new TopologyTestDriver(streamsBuilder.build(), props);

    TestInputTopic<byte[], JaegerSpanInternalModel.Span> inputTopic =
        td.createInputTopic(
            config.getString(
                org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants
                    .INPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().serializer(),
            new JaegerSpanSerde().serializer());

    TestOutputTopic<TraceIdentity, StructuredTrace> outputTopic =
        td.createOutputTopic(
            config.getString(RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY),
            traceIdentitySerde.deserializer(),
            defaultValueSerde.deserializer());

    String tenantId = "tenant1";

    JaegerSpanInternalModel.Span span =
        JaegerSpanInternalModel.Span.newBuilder()
            .setSpanId(ByteString.copyFrom("1".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("tenant-id")
                    .setVStr(tenantId)
                    .build())
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

    inputTopic.pipeInput(span);
    assertTrue(outputTopic.isEmpty());

    // pipe in one more span which match one of spanDropFilters (grpc.url)
    JaegerSpanInternalModel.Span span2 =
        JaegerSpanInternalModel.Span.newBuilder()
            .setSpanId(ByteString.copyFrom("3".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-3".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("tenant-id")
                    .setVStr(tenantId)
                    .build())
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

    inputTopic.pipeInput(span2);
    assertTrue(outputTopic.isEmpty());

    // pipe in one more span which match one of spanDropFilters (operation_name, and span.kind
    // not
    // exists)
    JaegerSpanInternalModel.Span span5 =
        JaegerSpanInternalModel.Span.newBuilder()
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
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "raw-spans-grouper")
  public void whenBypassedExpectStruturedTraceToBeOutput(@TempDir Path tempDir) {
    File file = tempDir.resolve("state").toFile();

    RawSpansGrouper underTest = new RawSpansGrouper(ConfigClientFactory.getClient());
    Config config =
        ConfigFactory.parseURL(
            getClass().getClassLoader().getResource("configs/raw-spans-grouper/application.conf"));

    Map<String, Object> baseProps = underTest.getBaseStreamsConfig();
    Map<String, Object> streamsProps = underTest.getStreamsConfig(config);
    baseProps.forEach(streamsProps::put);
    Map<String, Object> mergedProps = streamsProps;

    mergedProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    mergedProps.put(RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG, config);
    mergedProps.put(StreamsConfig.STATE_DIR_CONFIG, file.getAbsolutePath());

    StreamsBuilder streamsBuilder =
        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Properties props = new Properties();
    mergedProps.forEach(props::put);

    TopologyTestDriver td = new TopologyTestDriver(streamsBuilder.build(), props);

    TestInputTopic<byte[], JaegerSpanInternalModel.Span> inputTopic =
        td.createInputTopic(
            config.getString(
                org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants
                    .INPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().serializer(),
            new JaegerSpanSerde().serializer());

    Serde defaultValueSerde = new StreamsConfig(mergedProps).defaultValueSerde();
    Serde<TraceIdentity> traceIdentitySerde = new StreamsConfig(mergedProps).defaultKeySerde();

    Serde<TraceIdentity> spanIdentitySerde = new AvroSerde<>();
    spanIdentitySerde.configure(Map.of(), true);

    TestOutputTopic<TraceIdentity, StructuredTrace> outputTopic =
        td.createOutputTopic(
            config.getString(RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY),
            traceIdentitySerde.deserializer(),
            defaultValueSerde.deserializer());

    TestOutputTopic bypassOutputTopic =
        td.createOutputTopic(
            config.getString(SpanNormalizerConstants.BYPASS_OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(),
            defaultValueSerde.deserializer());

    TestOutputTopic rawLogOutputTopic =
        td.createOutputTopic(
            config.getString(SpanNormalizerConstants.OUTPUT_TOPIC_RAW_LOGS_CONFIG_KEY),
            spanIdentitySerde.deserializer(),
            defaultValueSerde.deserializer());

    // with logs event, with bypass key
    // expects no output to raw-span-grouper
    // expects output to trace-enricher
    // expects log output
    JaegerSpanInternalModel.Span span1 =
        JaegerSpanInternalModel.Span.newBuilder()
            .setSpanId(ByteString.copyFrom("1".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("tenant-id")
                    .setVStr("tenant-1")
                    .build())
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
                JaegerSpanInternalModel.Log.newBuilder()
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
    assertEquals("tenant-1", kv1.value.getCustomerId());
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
    // expects output to trace-enricher (after grouping)
    // expects no output to bypass output topic
    // expects log output
    JaegerSpanInternalModel.Span span2 =
        JaegerSpanInternalModel.Span.newBuilder()
            .setSpanId(ByteString.copyFrom("2".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-2".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("tenant-id")
                    .setVStr("tenant-1")
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addLogs(
                JaegerSpanInternalModel.Log.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(10).build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("z2")
                            .setVStr("some event detail")
                            .build()))
            .build();

    inputTopic.pipeInput(span2);
    td.advanceWallClockTime(Duration.ofSeconds(35));

    // validate that no output to bypass topic
    assertTrue(bypassOutputTopic.isEmpty());

    // validate that output to trace-enricher exists
    assertFalse(outputTopic.isEmpty());
    KeyValue<TraceIdentity, StructuredTrace> kv2 = outputTopic.readKeyValue();
    assertEquals("tenant-1", kv2.key.getTenantId());
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
    JaegerSpanInternalModel.Span span3 =
        JaegerSpanInternalModel.Span.newBuilder()
            .setSpanId(ByteString.copyFrom("3".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-3".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("tenant-id")
                    .setVStr("tenant-1")
                    .build())
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
                JaegerSpanInternalModel.Log.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(10).build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("z2")
                            .setVStr("some event detail")
                            .build()))
            .build();

    inputTopic.pipeInput(span3);
    td.advanceWallClockTime(Duration.ofSeconds(35));

    // validate that no output to bypass topic
    assertTrue(bypassOutputTopic.isEmpty());

    // validate that output to trace-enricher
    assertFalse(outputTopic.isEmpty());
    KeyValue<TraceIdentity, StructuredTrace> kv3 = outputTopic.readKeyValue();
    assertEquals("tenant-1", kv3.key.getTenantId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-3".getBytes()).toByteArray()),
        HexUtils.getHex(kv3.key.getTraceId().array()));

    // validate that no change in log traffic
    assertFalse(rawLogOutputTopic.isEmpty());
    logEvents = (LogEvents) rawLogOutputTopic.readKeyValue().value;
    Assertions.assertEquals(1, logEvents.getLogEvents().size());
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "raw-spans-grouper")
  public void testLaterArrivalJaegerSpans(@TempDir Path tempDir) {
    File file = tempDir.resolve("state").toFile();

    RawSpansGrouper underTest = new RawSpansGrouper(ConfigClientFactory.getClient());
    Config config =
        ConfigFactory.parseURL(
            getClass().getClassLoader().getResource("configs/raw-spans-grouper/application.conf"));

    Map<String, Object> baseProps = underTest.getBaseStreamsConfig();
    Map<String, Object> streamsProps = underTest.getStreamsConfig(config);
    baseProps.forEach(streamsProps::put);
    Map<String, Object> mergedProps = streamsProps;

    mergedProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    mergedProps.put(RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG, config);
    mergedProps.put(StreamsConfig.STATE_DIR_CONFIG, file.getAbsolutePath());

    StreamsBuilder streamsBuilder =
        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Properties props = new Properties();
    mergedProps.forEach(props::put);

    TopologyTestDriver td = new TopologyTestDriver(streamsBuilder.build(), props);

    TestInputTopic<byte[], JaegerSpanInternalModel.Span> inputTopic =
        td.createInputTopic(
            config.getString(
                org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants
                    .INPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().serializer(),
            new JaegerSpanSerde().serializer());

    Serde defaultValueSerde = new StreamsConfig(mergedProps).defaultValueSerde();
    Serde<TraceIdentity> traceIdentitySerde = new StreamsConfig(mergedProps).defaultKeySerde();

    Serde<TraceIdentity> spanIdentitySerde = new AvroSerde<>();
    spanIdentitySerde.configure(Map.of(), true);

    TestOutputTopic<TraceIdentity, StructuredTrace> outputTopic =
        td.createOutputTopic(
            config.getString(RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY),
            traceIdentitySerde.deserializer(),
            defaultValueSerde.deserializer());

    // case 1: within threshold, expect output
    Instant instant = Instant.now();
    JaegerSpanInternalModel.Span span =
        JaegerSpanInternalModel.Span.newBuilder()
            .setSpanId(ByteString.copyFrom("1".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
            .setStartTime(Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("tenant-id")
                    .setVStr("tenant-1")
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .build();
    inputTopic.pipeInput(span);
    td.advanceWallClockTime(Duration.ofSeconds(35));

    KeyValue<TraceIdentity, StructuredTrace> kv = outputTopic.readKeyValue();
    assertEquals("tenant-1", kv.key.getTenantId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-1".getBytes()).toByteArray()),
        HexUtils.getHex(kv.key.getTraceId().array()));

    // outside threshold, expect no output
    Instant instant1 = Instant.now().minus(25, ChronoUnit.HOURS);
    JaegerSpanInternalModel.Span span2 =
        JaegerSpanInternalModel.Span.newBuilder()
            .setSpanId(ByteString.copyFrom("2".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-2".getBytes()))
            .setStartTime(Timestamp.newBuilder().setSeconds(instant1.getEpochSecond()).build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .build();

    inputTopic.pipeInput(span2);
    td.advanceWallClockTime(Duration.ofSeconds(35));
    Assertions.assertTrue(outputTopic.isEmpty());
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "raw-spans-grouper")
  public void testTagsFilteringForJaegerSpans(@TempDir Path tempDir) {
    File file = tempDir.resolve("state").toFile();

    RawSpansGrouper underTest = new RawSpansGrouper(ConfigClientFactory.getClient());
    Config config =
        ConfigFactory.parseURL(
            getClass().getClassLoader().getResource("configs/raw-spans-grouper/application.conf"));

    Map<String, Object> baseProps = underTest.getBaseStreamsConfig();
    Map<String, Object> streamsProps = underTest.getStreamsConfig(config);
    baseProps.forEach(streamsProps::put);
    Map<String, Object> mergedProps = streamsProps;

    mergedProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    mergedProps.put(RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG, config);
    mergedProps.put(StreamsConfig.STATE_DIR_CONFIG, file.getAbsolutePath());

    StreamsBuilder streamsBuilder =
        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Properties props = new Properties();
    mergedProps.forEach(props::put);

    TopologyTestDriver td = new TopologyTestDriver(streamsBuilder.build(), props);

    TestInputTopic<byte[], JaegerSpanInternalModel.Span> inputTopic =
        td.createInputTopic(
            config.getString(
                org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants
                    .INPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().serializer(),
            new JaegerSpanSerde().serializer());

    Serde defaultValueSerde = new StreamsConfig(mergedProps).defaultValueSerde();
    Serde<TraceIdentity> traceIdentitySerde = new StreamsConfig(mergedProps).defaultKeySerde();

    Serde<TraceIdentity> spanIdentitySerde = new AvroSerde<>();
    spanIdentitySerde.configure(Map.of(), true);

    TestOutputTopic<TraceIdentity, StructuredTrace> outputTopic =
        td.createOutputTopic(
            config.getString(RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY),
            traceIdentitySerde.deserializer(),
            defaultValueSerde.deserializer());

    // makes sure that e2e works, so it tests basic scenario, rest of the
    // scenarios are covered in unit test of tagFilter
    // so configure for http extension attributes
    Instant instant = Instant.now();
    JaegerSpanInternalModel.Span span =
        JaegerSpanInternalModel.Span.newBuilder()
            .setSpanId(ByteString.copyFrom("1".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
            .setStartTime(Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("tenant-id")
                    .setVStr("tenant-1")
                    .build())
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
    td.advanceWallClockTime(Duration.ofSeconds(35));

    KeyValue<TraceIdentity, StructuredTrace> kv = outputTopic.readKeyValue();
    assertEquals("tenant-1", kv.key.getTenantId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-1".getBytes()).toByteArray()),
        HexUtils.getHex(kv.key.getTraceId().array()));
    StructuredTrace value = kv.value;

    assertEquals(
        HexUtils.getHex("1".getBytes()), HexUtils.getHex(value.getEventList().get(0).getEventId()));
    assertEquals(SERVICE_NAME, value.getEventList().get(0).getServiceName());

    // test of attributes
    Assertions.assertEquals(
        3, value.getEventList().get(0).getAttributes().getAttributeMap().size());

    Assertions.assertTrue(
        value
            .getEventList()
            .get(0)
            .getAttributes()
            .getAttributeMap()
            .containsKey("http.request.header.x-allowed-1"));
    Assertions.assertTrue(
        value
            .getEventList()
            .get(0)
            .getAttributes()
            .getAttributeMap()
            .containsKey("http.response.header.x-allowed-2"));

    Assertions.assertFalse(
        value
            .getEventList()
            .get(0)
            .getAttributes()
            .getAttributeMap()
            .containsKey("http.request.header.x-not-allowed-1"));
    Assertions.assertFalse(
        value
            .getEventList()
            .get(0)
            .getAttributes()
            .getAttributeMap()
            .containsKey("http.response.header.x-not-allowed-2"));
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "raw-spans-grouper")
  public void whenRawSpansAreReceivedWithInactivityExpectTraceToBeOutput(@TempDir Path tempDir) {
    File file = tempDir.resolve("state").toFile();

    RawSpansGrouper underTest = new RawSpansGrouper(ConfigClientFactory.getClient());
    Config config =
        ConfigFactory.parseURL(
            getClass().getClassLoader().getResource("configs/raw-spans-grouper/application.conf"));

    Map<String, Object> baseProps = underTest.getBaseStreamsConfig();
    Map<String, Object> streamsProps = underTest.getStreamsConfig(config);
    baseProps.forEach(streamsProps::put);
    Map<String, Object> mergedProps = streamsProps;

    mergedProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    mergedProps.put(RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG, config);
    mergedProps.put(StreamsConfig.STATE_DIR_CONFIG, file.getAbsolutePath());

    StreamsBuilder streamsBuilder =
        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Properties props = new Properties();
    mergedProps.forEach(props::put);

    Serde defaultValueSerde = new StreamsConfig(mergedProps).defaultValueSerde();

    Serde<TraceIdentity> traceIdentitySerde = new StreamsConfig(mergedProps).defaultKeySerde();

    TopologyTestDriver td = new TopologyTestDriver(streamsBuilder.build(), props);

    TestInputTopic<byte[], JaegerSpanInternalModel.Span> inputTopic =
        td.createInputTopic(
            config.getString(
                org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants
                    .INPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().serializer(),
            new JaegerSpanSerde().serializer());

    TestOutputTopic<TraceIdentity, StructuredTrace> outputTopic =
        td.createOutputTopic(
            config.getString(RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY),
            traceIdentitySerde.deserializer(),
            defaultValueSerde.deserializer());

    String tenantId = "tenant1";

    // create spans for trace-1 of tenant1
    JaegerSpanInternalModel.Span span1 = createSpan("tenant1", "trace-1", "event-1");
    JaegerSpanInternalModel.Span span2 = createSpan("tenant1", "trace-1", "event-2");
    JaegerSpanInternalModel.Span span3 = createSpan("tenant1", "trace-1", "event-3");

    // create spans for trace-2 of tenant1
    JaegerSpanInternalModel.Span span4 = createSpan("tenant1", "trace-2", "event-4");
    JaegerSpanInternalModel.Span span5 = createSpan("tenant1", "trace-2", "event-5");

    // create spans for trace-3 of tenant1
    JaegerSpanInternalModel.Span span6 = createSpan("tenant1", "trace-3", "event-6");
    JaegerSpanInternalModel.Span span7 = createSpan("tenant1", "trace-3", "event-7");
    JaegerSpanInternalModel.Span span8 = createSpan("tenant1", "trace-3", "event-8");
    JaegerSpanInternalModel.Span span9 = createSpan("tenant1", "trace-3", "event-9");
    JaegerSpanInternalModel.Span span10 = createSpan("tenant1", "trace-3", "event-10");
    JaegerSpanInternalModel.Span span11 = createSpan("tenant1", "trace-3", "event-11");

    // create 8 spans for tenant-2 for trace-4
    JaegerSpanInternalModel.Span span12 = createSpan("tenant2", "trace-4", "event-12");
    JaegerSpanInternalModel.Span span13 = createSpan("tenant2", "trace-4", "event-13");
    JaegerSpanInternalModel.Span span14 = createSpan("tenant2", "trace-4", "event-14");
    JaegerSpanInternalModel.Span span15 = createSpan("tenant2", "trace-4", "event-15");
    JaegerSpanInternalModel.Span span16 = createSpan("tenant2", "trace-4", "event-16");
    JaegerSpanInternalModel.Span span17 = createSpan("tenant2", "trace-4", "event-17");
    JaegerSpanInternalModel.Span span18 = createSpan("tenant2", "trace-4", "event-18");
    JaegerSpanInternalModel.Span span19 = createSpan("tenant2", "trace-4", "event-19");

    inputTopic.pipeInput(tenantId.getBytes(), span1);
    inputTopic.pipeInput(tenantId.getBytes(), span4);
    td.advanceWallClockTime(Duration.ofSeconds(1));
    inputTopic.pipeInput(tenantId.getBytes(), span2);

    // select a value < 30s (groupingWindowTimeoutInMs)
    // this shouldn't trigger a punctuate call
    td.advanceWallClockTime(Duration.ofMillis(200));
    assertTrue(outputTopic.isEmpty());

    // the next advance should trigger a punctuate call and emit a trace with 2 spans
    td.advanceWallClockTime(Duration.ofSeconds(32));

    // trace1 should have 2 span span1, span2
    StructuredTrace trace = outputTopic.readValue();
    assertEquals(2, trace.getEventList().size());
    Set<String> traceEventIds =
        trace.getEventList().stream()
            .map(id -> new String(id.getEventId().array()))
            .collect(Collectors.toSet());
    assertTrue(traceEventIds.contains("event-1"));
    assertTrue(traceEventIds.contains("event-2"));

    // trace2 should have 1 span span3
    trace = outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals("event-4", new String(trace.getEventList().get(0).getEventId().array()));

    inputTopic.pipeInput(tenantId.getBytes(), span3);
    td.advanceWallClockTime(Duration.ofSeconds(45));
    inputTopic.pipeInput(tenantId.getBytes(), span5);

    // the next advance should trigger a punctuate call and emit a trace with 2 spans
    td.advanceWallClockTime(Duration.ofSeconds(35));

    // trace1 should have 1 span i.e. span3
    trace = outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals("event-3", new String(trace.getEventList().get(0).getEventId().array()));

    // trace2 should have 1 span i.e. span4
    trace = outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals("event-5", new String(trace.getEventList().get(0).getEventId().array()));

    inputTopic.pipeInput(tenantId.getBytes(), span6);
    inputTopic.pipeInput(tenantId.getBytes(), span7);
    inputTopic.pipeInput(tenantId.getBytes(), span8);
    inputTopic.pipeInput(tenantId.getBytes(), span9);
    inputTopic.pipeInput(tenantId.getBytes(), span10);
    inputTopic.pipeInput(tenantId.getBytes(), span11);

    td.advanceWallClockTime(Duration.ofSeconds(35));

    // trace should be truncated with 5 spans
    trace = outputTopic.readValue();
    assertEquals(5, trace.getEventList().size());

    // input 8 spans of trace-4 for tenant2, as there is global upper limit apply, it will emit only
    // 6

    String tenantId2 = "tenant2";
    inputTopic.pipeInput(tenantId2.getBytes(), span12);
    inputTopic.pipeInput(tenantId2.getBytes(), span13);
    inputTopic.pipeInput(tenantId2.getBytes(), span14);
    inputTopic.pipeInput(tenantId2.getBytes(), span15);
    inputTopic.pipeInput(tenantId2.getBytes(), span16);
    inputTopic.pipeInput(tenantId2.getBytes(), span17);
    inputTopic.pipeInput(tenantId2.getBytes(), span18);
    inputTopic.pipeInput(tenantId2.getBytes(), span19);

    td.advanceWallClockTime(Duration.ofSeconds(35));

    TestRecord<TraceIdentity, StructuredTrace> testRecord = outputTopic.readRecord();

    assertEquals(tenantId2, testRecord.getKey().getTenantId());
    assertEquals(6, testRecord.getValue().getEventList().size());
  }

  private JaegerSpanInternalModel.Span createSpan(String tenantId, String traceId, String eventId) {
    return JaegerSpanInternalModel.Span.newBuilder()
        .setSpanId(ByteString.copyFrom(eventId.getBytes()))
        .setTraceId(ByteString.copyFrom(traceId.getBytes()))
        .addTags(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setKey("jaeger.servicename")
                .setVStr(SERVICE_NAME)
                .build())
        .addTags(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setKey("tenant-id")
                .setVStr(tenantId)
                .build())
        .addLogs(
            JaegerSpanInternalModel.Log.newBuilder()
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
            JaegerSpanInternalModel.Log.newBuilder()
                .setTimestamp(Timestamp.newBuilder().setSeconds(10).build())
                .addFields(
                    JaegerSpanInternalModel.KeyValue.newBuilder()
                        .setKey("z2")
                        .setVStr("some event detail")
                        .build()))
        .build();
  }

  private Event createEvent(String eventId, String tenantId) {
    return Event.newBuilder()
        .setCustomerId(tenantId)
        .setEventId(ByteBuffer.wrap(eventId.getBytes()))
        .setStartTimeMillis(System.currentTimeMillis())
        .build();
  }

  private TraceIdentity createTraceIdentity(String tenantId, String traceId) {
    return TraceIdentity.newBuilder()
        .setTenantId(tenantId)
        .setTraceId(ByteBuffer.wrap(traceId.getBytes()))
        .build();
  }
}
