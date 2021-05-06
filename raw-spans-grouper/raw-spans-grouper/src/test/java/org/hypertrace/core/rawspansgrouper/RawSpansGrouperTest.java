package org.hypertrace.core.rawspansgrouper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class RawSpansGrouperTest {

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
    TestInputTopic<TraceIdentity, RawSpan> inputTopic =
        td.createInputTopic(
            config.getString(RawSpanGrouperConstants.INPUT_TOPIC_CONFIG_KEY),
            traceIdentitySerde.serializer(),
            defaultValueSerde.serializer());

    TestOutputTopic outputTopic =
        td.createOutputTopic(
            config.getString(RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(),
            defaultValueSerde.deserializer());

    String tenantId = "tenant1";

    RawSpan span1 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
            .setCustomerId("tenant1")
            .setEvent(createEvent("event-1", "tenant1"))
            .build();
    RawSpan span2 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
            .setCustomerId("tenant1")
            .setEvent(createEvent("event-2", "tenant1"))
            .build();
    RawSpan span3 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
            .setCustomerId("tenant1")
            .setEvent(createEvent("event-3", "tenant1"))
            .build();
    RawSpan span4 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-2".getBytes()))
            .setCustomerId("tenant1")
            .setEvent(createEvent("event-4", "tenant1"))
            .build();
    RawSpan span5 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-2".getBytes()))
            .setCustomerId("tenant1")
            .setEvent(createEvent("event-5", "tenant1"))
            .build();
    RawSpan span6 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-3".getBytes()))
            .setCustomerId("tenant1")
            .setEvent(createEvent("event-6", "tenant1"))
            .build();
    RawSpan span7 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-3".getBytes()))
            .setCustomerId("tenant1")
            .setEvent(createEvent("event-7", "tenant1"))
            .build();
    RawSpan span8 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-3".getBytes()))
            .setCustomerId("tenant1")
            .setEvent(createEvent("event-8", "tenant1"))
            .build();
    RawSpan span9 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-3".getBytes()))
            .setCustomerId("tenant1")
            .setEvent(createEvent("event-9", "tenant1"))
            .build();
    RawSpan span10 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-3".getBytes()))
            .setCustomerId("tenant1")
            .setEvent(createEvent("event-10", "tenant1"))
            .build();
    RawSpan span11 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-3".getBytes()))
            .setCustomerId("tenant1")
            .setEvent(createEvent("event-11", "tenant1"))
            .build();

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-1"), span1);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-2"), span4);
    td.advanceWallClockTime(Duration.ofSeconds(1));
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-1"), span2);

    // select a value < 30s (groupingWindowTimeoutInMs)
    // this shouldn't trigger a punctuate call
    td.advanceWallClockTime(Duration.ofMillis(200));
    assertTrue(outputTopic.isEmpty());

    // the next advance should trigger a punctuate call and emit a trace with 2 spans
    td.advanceWallClockTime(Duration.ofSeconds(32));

    // trace1 should have 2 span span1, span2
    StructuredTrace trace = (StructuredTrace) outputTopic.readValue();
    assertEquals(2, trace.getEventList().size());
    assertEquals("event-1", HexUtils.getHex(trace.getEventList().get(0).getEventId()));
    assertEquals("event-2", HexUtils.getHex(trace.getEventList().get(1).getEventId()));

    // trace2 should have 1 span span3
    trace = (StructuredTrace) outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals("event-4", HexUtils.getHex(trace.getEventList().get(0).getEventId()));

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-1"), span3);
    td.advanceWallClockTime(Duration.ofSeconds(45));
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-2"), span5);
    // the next advance should trigger a punctuate call and emit a trace with 2 spans
    td.advanceWallClockTime(Duration.ofSeconds(35));

    // trace1 should have 1 span i.e. span3
    trace = (StructuredTrace) outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals("event-3", HexUtils.getHex(trace.getEventList().get(0).getEventId()));

    // trace2 should have 1 span i.e. span4
    trace = (StructuredTrace) outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals("event-5", HexUtils.getHex(trace.getEventList().get(0).getEventId()));

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span6);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span7);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span8);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span9);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span10);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span11);
    td.advanceWallClockTime(Duration.ofSeconds(35));

    // trace should be truncated with 5 spans
    trace = (StructuredTrace) outputTopic.readValue();
    assertEquals(5, trace.getEventList().size());
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
