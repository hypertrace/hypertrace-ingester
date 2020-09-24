package org.hypertrace.core.rawspansgrouper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class RawSpansGrouperTest {

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "raw-spans-grouper")
  public void whenRawSpansAreReceivedWithInactivityExpectTraceToBeOutput(@TempDir Path tempDir) {
    File file = tempDir.resolve("state").toFile();

    RawSpansGrouper underTest = new RawSpansGrouper(ConfigClientFactory.getClient());
    Config config = ConfigFactory.parseURL(
        getClass().getClassLoader().getResource("configs/raw-spans-grouper/application.conf"));

    Map<String, Object> baseProps = underTest.getBaseStreamsConfig();
    Map<String, Object> streamsProps = underTest.getStreamsConfig(config);
    baseProps.forEach(streamsProps::put);
    Map<String, Object> mergedProps = streamsProps;

    mergedProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    mergedProps.put(RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG, config);
    mergedProps.put(StreamsConfig.STATE_DIR_CONFIG, file.getAbsolutePath());

    StreamsBuilder streamsBuilder = underTest
        .buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Properties props = new Properties();
    mergedProps.forEach(props::put);

    Serde defaultValueSerde = new StreamsConfig(mergedProps).defaultValueSerde();

    TopologyTestDriver td = new TopologyTestDriver(streamsBuilder.build(), props);
    TestInputTopic<String, RawSpan> inputTopic = td
        .createInputTopic(config.getString(RawSpanGrouperConstants.INPUT_TOPIC_CONFIG_KEY),
            Serdes.String().serializer(), defaultValueSerde.serializer());

    TestOutputTopic outputTopic = td
        .createOutputTopic(config.getString(RawSpanGrouperConstants.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(), defaultValueSerde.deserializer());

    RawSpan span1 = RawSpan.newBuilder().setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
        .setCustomerId("customer1").setEvent(createEvent("event-1", "customer1")).build();
    RawSpan span2 = RawSpan.newBuilder().setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
        .setCustomerId("customer1").setEvent(createEvent("event-2", "customer1")).build();
    RawSpan span3 = RawSpan.newBuilder().setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
        .setCustomerId("customer1").setEvent(createEvent("event-3", "customer1")).build();
    RawSpan span4 = RawSpan.newBuilder().setTraceId(ByteBuffer.wrap("trace-2".getBytes()))
        .setCustomerId("customer1").setEvent(createEvent("event-4", "customer1")).build();
    RawSpan span5 = RawSpan.newBuilder().setTraceId(ByteBuffer.wrap("trace-2".getBytes()))
        .setCustomerId("customer1").setEvent(createEvent("event-5", "customer1")).build();

    inputTopic.pipeInput("trace-1", span1);
    inputTopic.pipeInput("trace-2", span4);
    td.advanceWallClockTime(Duration.ofSeconds(1));
    inputTopic.pipeInput("trace-1", span2);

    // select a value < 30s (groupingWindowTimeoutInMs)
    // this shouldn't trigger a punctuate call
    td.advanceWallClockTime(Duration.ofMillis(200));
    assertTrue(outputTopic.isEmpty());

    // the next advance should trigger a punctuate call and emit a trace with 2 spans
    td.advanceWallClockTime(Duration.ofSeconds(32));

    // trace1 should have 2 span span1, span2
    StructuredTrace trace = (StructuredTrace) outputTopic.readValue();
    assertEquals(2, trace.getEventList().size());
    assertEquals(ByteBuffer.wrap("event-1".getBytes()), trace.getEventList().get(0).getEventId());
    assertEquals(ByteBuffer.wrap("event-2".getBytes()), trace.getEventList().get(1).getEventId());

    // trace2 should have 1 span span3
    trace = (StructuredTrace) outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals(ByteBuffer.wrap("event-4".getBytes()), trace.getEventList().get(0).getEventId());

    inputTopic.pipeInput("trace-1", span3);
    td.advanceWallClockTime(Duration.ofSeconds(45));
    inputTopic.pipeInput("trace-2", span5);
    // the next advance should trigger a punctuate call and emit a trace with 2 spans
    td.advanceWallClockTime(Duration.ofSeconds(35));

    // trace1 should have 1 span i.e. span3
    trace = (StructuredTrace) outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals(ByteBuffer.wrap("event-3".getBytes()), trace.getEventList().get(0).getEventId());

    // trace2 should have 1 span i.e. span4
    trace = (StructuredTrace) outputTopic.readValue();
    assertEquals(1, trace.getEventList().size());
    assertEquals(ByteBuffer.wrap("event-5".getBytes()), trace.getEventList().get(0).getEventId());
  }

  private Event createEvent(String eventId, String customerId) {
    return Event.newBuilder().setCustomerId(customerId)
        .setEventId(ByteBuffer.wrap(eventId.getBytes()))
        .setStartTimeMillis(System.currentTimeMillis()).build();
  }
}