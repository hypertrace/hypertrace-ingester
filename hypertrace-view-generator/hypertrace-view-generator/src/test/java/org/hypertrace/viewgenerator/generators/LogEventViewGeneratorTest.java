package org.hypertrace.viewgenerator.generators;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.LogEvent;
import org.hypertrace.core.datamodel.LogEvents;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.viewgenerator.api.LogEventView;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LogEventViewGeneratorTest {

  @Test
  public void testProcess_emptyLogEvents() {
    LogEvents logEvents =
        LogEvents.newBuilder()
            .setLogEvents(Collections.singletonList(LogEvent.newBuilder().build()))
            .build();

    List<LogEventView> list = new LogEventViewGenerator().process(logEvents);
    // empty record is generated
    Assertions.assertFalse(list.isEmpty());
  }

  @Test
  public void testProcess_emptyAttributes() {
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
  public void testProcess_allFieldsPresent() {
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
}
