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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
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

    TestOutputTopic<TraceIdentity, StructuredTrace> outputTopic =
        td.createOutputTopic(
            config.getString(RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(),
            defaultValueSerde.deserializer());

    String tenantId = "tenant1";

    // create spans for trace-1 of tenant1
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

    // create spans for trace-2 of tenant1
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

    // create spans for trace-3 of tenant1
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

    // create 8 spans for tenant-2 for trace-4
    String tenant2 = "tenant2";
    RawSpan span12 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-4".getBytes()))
            .setCustomerId(tenant2)
            .setEvent(createEvent("event-12", tenant2))
            .build();
    RawSpan span13 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-4".getBytes()))
            .setCustomerId(tenant2)
            .setEvent(createEvent("event-13", tenant2))
            .build();
    RawSpan span14 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-4".getBytes()))
            .setCustomerId(tenant2)
            .setEvent(createEvent("event-14", tenant2))
            .build();
    RawSpan span15 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-4".getBytes()))
            .setCustomerId(tenant2)
            .setEvent(createEvent("event-15", tenant2))
            .build();
    RawSpan span16 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-4".getBytes()))
            .setCustomerId(tenant2)
            .setEvent(createEvent("event-16", tenant2))
            .build();
    RawSpan span17 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-4".getBytes()))
            .setCustomerId(tenant2)
            .setEvent(createEvent("event-17", tenant2))
            .build();
    RawSpan span18 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-4".getBytes()))
            .setCustomerId(tenant2)
            .setEvent(createEvent("event-18", tenant2))
            .build();
    RawSpan span19 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-4".getBytes()))
            .setCustomerId(tenant2)
            .setEvent(createEvent("event-19", tenant2))
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

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-1"), span3);
    td.advanceWallClockTime(Duration.ofSeconds(45));
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-2"), span5);
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

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span6);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span7);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span8);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span9);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span10);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span11);
    td.advanceWallClockTime(Duration.ofSeconds(35));

    // trace should be truncated with 5 spans
    trace = outputTopic.readValue();
    assertEquals(5, trace.getEventList().size());

    // input 8 spans of trace-4 for tenant2, as there is global upper limit apply, it will emit only
    // 6
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span12);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span13);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span14);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span15);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span16);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span17);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span18);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span19);
    td.advanceWallClockTime(Duration.ofSeconds(35));

    TestRecord<TraceIdentity, StructuredTrace> testRecord = outputTopic.readRecord();

    assertEquals(tenant2, testRecord.getKey().getTenantId());
    assertEquals(6, testRecord.getValue().getEventList().size());
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
