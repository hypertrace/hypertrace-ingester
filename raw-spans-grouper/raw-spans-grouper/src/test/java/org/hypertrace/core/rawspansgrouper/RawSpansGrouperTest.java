package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants.PEER_SERVICE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
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

    Clock clock = mock(Clock.class);
    StreamsBuilder streamsBuilder =
        underTest.buildTopologyWithClock(clock, mergedProps, new StreamsBuilder(), new HashMap<>());

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
            traceIdentitySerde.deserializer(),
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

    // dummyTenant is used to advance stream time as required, all spans will be dropped
    // because of setting max span count to 0, no traces for this tenant will be emitted
    String dummyTenant = "dummyTenant";
    TraceIdentity dummyTraceIdentity = createTraceIdentity(dummyTenant, "dummyTrace");
    RawSpan dummySpan =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("dummyTrace".getBytes()))
            .setCustomerId(tenant2)
            .setEvent(createEvent("dummyEvent", dummyTenant))
            .build();

    long messageTime = advanceAndSyncClockMock(0, clock, 0);

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-1"), span1, messageTime);
    messageTime = advanceAndSyncClockMock(messageTime, clock, 1000);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-1"), span2, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-2"), span4, messageTime);

    // select a value < 30s (groupingWindowTimeoutInMs)
    // this shouldn't trigger a span emit
    messageTime = advanceAndSyncClockMock(messageTime, clock, 200);
    inputTopic.pipeInput(dummyTraceIdentity, dummySpan, messageTime);
    assertTrue(outputTopic.isEmpty());

    // the next advance should and emit a trace1 with 2 spans, trace2 with one span
    messageTime = advanceAndSyncClockMock(messageTime, clock, 35_000);
    inputTopic.pipeInput(dummyTraceIdentity, dummySpan, messageTime);

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

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-1"), span3, messageTime);
    messageTime = advanceAndSyncClockMock(messageTime, clock, 45_000);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-2"), span5, messageTime);
    // the next advance should emit a trace1, trace2 with one span
    messageTime = advanceAndSyncClockMock(messageTime, clock, 35_000);
    inputTopic.pipeInput(dummyTraceIdentity, dummySpan, messageTime);

    // trace1 should have 1 span i.e. span3
    trace = outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals("event-3", new String(trace.getEventList().get(0).getEventId().array()));

    // trace2 should have 1 span i.e. span4
    trace = outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals("event-5", new String(trace.getEventList().get(0).getEventId().array()));

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span6, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span7, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span8, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span9, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span10, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span11, messageTime);
    // the next advance should emit trace3
    messageTime = advanceAndSyncClockMock(messageTime, clock, 35_000);
    inputTopic.pipeInput(dummyTraceIdentity, dummySpan, messageTime);

    // trace3 should be truncated with 5 spans because of tenant limit
    trace = outputTopic.readValue();
    assertEquals(5, trace.getEventList().size());

    // input 8 spans of trace-4 for tenant2, as there is global upper limit apply, it will emit only
    // 6
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span12, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span13, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span14, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span15, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span16, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span17, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span18, messageTime);
    inputTopic.pipeInput(createTraceIdentity(tenant2, "trace-4"), span19, messageTime);
    // the next advance should emit trace 4
    messageTime = advanceAndSyncClockMock(messageTime, clock, 35_000);
    inputTopic.pipeInput(dummyTraceIdentity, dummySpan, messageTime);

    TestRecord<TraceIdentity, StructuredTrace> testRecord = outputTopic.readRecord();

    assertEquals(tenant2, testRecord.getKey().getTenantId());
    assertEquals(6, testRecord.getValue().getEventList().size());
  }

  private long advanceAndSyncClockMock(long messageTime, Clock clock, long advanceMs) {
    long finalMessageTime = messageTime + advanceMs;
    when(clock.millis()).thenAnswer((inv) -> finalMessageTime);
    return finalMessageTime;
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "raw-spans-grouper")
  void testMirroringSpansGrouping(@TempDir Path tempDir) {
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
            traceIdentitySerde.deserializer(),
            defaultValueSerde.deserializer());

    String hostAddr1 = "1.2.3.4";
    String hostAddr2 = "1.2.3.5";
    String hostAddr3 = "1.2.3.6";
    String hostPort2 = "5000";
    String hostPort3 = "6000";
    String service1 = "service1";
    String service2 = "service2";
    String service3 = "service3";

    String tenantId = "tenant1";
    RawSpan span1 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
            .setCustomerId(tenantId)
            .setEvent(
                createMirroringEvent(
                    "event-1", tenantId, service1, "client", hostAddr1, "1234", hostAddr2,
                    hostPort2))
            .build();
    RawSpan span2 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-2".getBytes()))
            .setCustomerId(tenantId)
            .setEvent(
                createMirroringEvent(
                    "event-2", tenantId, service3, "server", hostAddr3, hostPort3, hostAddr2,
                    "5678"))
            .build();
    RawSpan span3 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-3".getBytes()))
            .setCustomerId(tenantId)
            .setEvent(
                createMirroringEvent(
                    "event-3", tenantId, service2, "server", hostAddr2, hostPort2, hostAddr1,
                    "1234"))
            .build();
    RawSpan span4 =
        RawSpan.newBuilder()
            .setTraceId(ByteBuffer.wrap("trace-4".getBytes()))
            .setCustomerId(tenantId)
            .setEvent(
                createMirroringEvent(
                    "event-4", tenantId, service2, "client", hostAddr2, "5678", hostAddr3,
                    hostPort3))
            .build();

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-1"), span1);
    StructuredTrace trace = outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals(span1.getEvent(), trace.getEventList().get(0));

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-2"), span2);
    trace = outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals(span2.getEvent(), trace.getEventList().get(0));

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-3"), span3);
    trace = outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    Event event = span3.getEvent();
    event.setEnrichedAttributes(
        Attributes.newBuilder()
            .setAttributeMap(Map.of(PEER_SERVICE_NAME, createAttribute(service1)))
            .build());
    assertEquals(event, trace.getEventList().get(0));

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-4"), span4);
    trace = outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals(span4.getEvent(), trace.getEventList().get(0));

    inputTopic.pipeInput(createTraceIdentity(tenantId, "trace-2"), span2);
    trace = outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    event = span2.getEvent();
    event.setEnrichedAttributes(
        Attributes.newBuilder()
            .setAttributeMap(Map.of(PEER_SERVICE_NAME, createAttribute(service2)))
            .build());
    assertEquals(event, trace.getEventList().get(0));
  }

  private Event createEvent(String eventId, String tenantId) {
    return Event.newBuilder()
        .setCustomerId(tenantId)
        .setEventId(ByteBuffer.wrap(eventId.getBytes()))
        .setAttributes(Attributes.newBuilder().setAttributeMap(Collections.emptyMap()).build())
        .setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(Collections.emptyMap()).build())
        .setStartTimeMillis(System.currentTimeMillis())
        .build();
  }

  private Event createMirroringEvent(
      String eventId,
      String tenantId,
      String service,
      String spanKind,
      String hostAddr,
      String hostPort,
      String peerAddr,
      String peerPort) {
    return Event.newBuilder()
        .setCustomerId(tenantId)
        .setServiceName(service)
        .setEventId(ByteBuffer.wrap(eventId.getBytes()))
        .setStartTimeMillis(System.currentTimeMillis())
        .setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(Collections.emptyMap()).build())
        .setAttributes(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        "agent.type",
                        createAttribute("mirror"),
                        "deployment.environment",
                        createAttribute("environment"),
                        "span.kind",
                        createAttribute(spanKind),
                        "net.sock.host.addr",
                        createAttribute(hostAddr),
                        "net.sock.host.port",
                        createAttribute(hostPort),
                        "net.sock.peer.addr",
                        createAttribute(peerAddr),
                        "net.sock.peer.port",
                        createAttribute(peerPort)))
                .build())
        .build();
  }

  private AttributeValue createAttribute(String value) {
    return AttributeValue.newBuilder().setValue(value).build();
  }

  private TraceIdentity createTraceIdentity(String tenantId, String traceId) {
    return TraceIdentity.newBuilder()
        .setTenantId(tenantId)
        .setTraceId(ByteBuffer.wrap(traceId.getBytes()))
        .build();
  }
}
