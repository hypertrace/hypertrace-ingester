package org.hypertrace.viewgenerator.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.LogEvent;
import org.hypertrace.core.datamodel.LogEvents;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.viewgenerator.api.LogEventView;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LogEventViewGeneratorTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void testProcess_emptyLogEvents() {
    LogEvents logEvents =
        LogEvents.newBuilder()
            .setLogEvents(Collections.singletonList(LogEvent.newBuilder().build()))
            .build();

    List<LogEventView> list = new LogEventViewGenerator().process(logEvents);
    // empty record is generated
    Assertions.assertFalse(list.isEmpty());
    assertEquals(0, list.get(0).getTimestampNanos());
    Assertions.assertNull(list.get(0).getTraceId());
    Assertions.assertNull(list.get(0).getSpanId());
    Assertions.assertNull(list.get(0).getAttributes());
    Assertions.assertNull(list.get(0).getTenantId());
  }

  @Test
  void testProcess_emptyAttributes() {
    LogEvents logEvents =
        LogEvents.newBuilder()
            .setLogEvents(
                Collections.singletonList(LogEvent.newBuilder().setTenantId("tenant-1").build()))
            .build();

    List<LogEventView> list = new LogEventViewGenerator().process(logEvents);
    Assertions.assertNotNull(list.get(0).getTenantId());
    Assertions.assertNull(list.get(0).getAttributes());
  }

  @Test
  void testProcess_allFieldsPresent() {
    LogEvents logEvents =
        LogEvents.newBuilder()
            .setLogEvents(
                Collections.singletonList(
                    LogEvent.newBuilder()
                        .setTenantId("tenant-1")
                        .setTimestampNanos(System.nanoTime())
                        .setSpanId(ByteBuffer.wrap("span".getBytes()))
                        .setTraceId(ByteBuffer.wrap("trace".getBytes()))
                        .setAttributes(
                            Attributes.newBuilder()
                                .setAttributeMap(
                                    Map.of("some-attribute", AttributeValueCreator.create(10)))
                                .build())
                        .build()))
            .build();

    List<LogEventView> list = new LogEventViewGenerator().process(logEvents);
    Assertions.assertNotNull(list.get(0).getTenantId());
    Assertions.assertNotNull(list.get(0).getSpanId());
    Assertions.assertNotNull(list.get(0).getTraceId());
    Assertions.assertTrue(list.get(0).getTimestampNanos() != 0);
    Assertions.assertNotNull(list.get(0).getAttributes());
  }

  @Test
  void testProcess_attributeMap() throws JsonProcessingException {
    LogEvents logEvents =
        LogEvents.newBuilder()
            .setLogEvents(
                Collections.singletonList(
                    LogEvent.newBuilder()
                        .setTenantId("tenant-1")
                        .setTimestampNanos(System.nanoTime())
                        .setSpanId(ByteBuffer.wrap("span".getBytes()))
                        .setTraceId(ByteBuffer.wrap("trace".getBytes()))
                        .setAttributes(
                            Attributes.newBuilder()
                                .setAttributeMap(
                                    Map.of(
                                        "k1", AttributeValueCreator.create(10),
                                        "k2", AttributeValueCreator.create(20)))
                                .build())
                        .build()))
            .build();

    List<LogEventView> list = new LogEventViewGenerator().process(logEvents);
    Map<String, String> deserializedMap =
        OBJECT_MAPPER.readValue(list.get(0).getAttributes(), HashMap.class);

    assertEquals("10", deserializedMap.get("k1"));
    assertEquals("20", deserializedMap.get("k2"));
  }

  @Test
  void testSummaryField() {
    LogEventViewGenerator logEventViewGenerator = new LogEventViewGenerator();
    Map<String, String> attributes = new HashMap<>();
    List<String> summaryKeys = new ArrayList<>(LogEventViewGenerator.SUMMARY_KEYS);
    AtomicInteger attributeVal = new AtomicInteger();
    summaryKeys.forEach(key -> attributes.put(key, String.valueOf(attributeVal.getAndIncrement())));

    for (String summaryKey : LogEventViewGenerator.SUMMARY_KEYS) {
      LogEvents logEvents = getLogEventsWithAttribute(attributes);
      List<LogEventView> list = logEventViewGenerator.process(logEvents);
      assertEquals(attributes.get(summaryKey), list.get(0).getSummary());
      attributes.remove(summaryKey);
    }
  }

  private LogEvents getLogEventsWithAttribute(Map<String, String> attributes) {
    Map<String, AttributeValue> map =
        attributes.entrySet().stream()
            .collect(
                Collectors.toMap(Entry::getKey, v -> AttributeValueCreator.create(v.getValue())));
    return LogEvents.newBuilder()
        .setLogEvents(
            Collections.singletonList(
                LogEvent.newBuilder()
                    .setTenantId("tenant-1")
                    .setTimestampNanos(System.nanoTime())
                    .setSpanId(ByteBuffer.wrap("span".getBytes()))
                    .setTraceId(ByteBuffer.wrap("trace".getBytes()))
                    .setAttributes(Attributes.newBuilder().setAttributeMap(map).build())
                    .build()))
        .build();
  }
}
