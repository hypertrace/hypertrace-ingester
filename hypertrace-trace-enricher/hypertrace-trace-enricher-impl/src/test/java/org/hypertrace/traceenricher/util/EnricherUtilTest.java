package org.hypertrace.traceenricher.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.traceenricher.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EnricherUtilTest {

  @Test
  public void testGrpcEventNames() {
    Event e1 = mock(Event.class);
    when(e1.getEventName()).thenReturn("Sent./products/browse");
    assertTrue(EnricherUtil.isSentGrpcEvent(e1));

    Event e2 = mock(Event.class);
    when(e2.getEventName()).thenReturn("Recv./products/browse");
    assertTrue(EnricherUtil.isReceivedGrpcEvent(e2));

    Event e3 = mock(Event.class);
    when(e3.getEventName()).thenReturn("GET /products/browse");
    assertFalse(EnricherUtil.isSentGrpcEvent(e3));
    assertFalse(EnricherUtil.isReceivedGrpcEvent(e3));
  }

  @Test
  public void testSetAttributeForFirstExistingKey() {
    Event e = mock(Event.class);
    Attributes attributes =
        Attributes.newBuilder()
            .setAttributeMap(
                Map.of(
                    "a",
                    TestUtil.buildAttributeValue("a-value"),
                    "b",
                    TestUtil.buildAttributeValue("b-value")))
            .build();
    when(e.getAttributes()).thenReturn(attributes);

    Builder entityBuilder = Entity.newBuilder();
    EnricherUtil.setAttributeForFirstExistingKey(e, entityBuilder, Arrays.asList("a", "b", "c"));
    Assertions.assertTrue(entityBuilder.getAttributesMap().containsKey("a"));
  }

  @Test
  public void testGetAttribute() {
    Attributes attributes =
        Attributes.newBuilder()
            .setAttributeMap(
                Map.of(
                    "a",
                    TestUtil.buildAttributeValue("a-value"),
                    "b",
                    TestUtil.buildAttributeValue("b-value")))
            .build();
    Optional<AttributeValue> val = EnricherUtil.getAttribute(attributes, "a");
    Assertions.assertEquals("a-value", val.get().getValue());
    val = EnricherUtil.getAttribute(attributes, "c");
    Assertions.assertTrue(val.isEmpty());

    attributes = null;
    val = EnricherUtil.getAttribute(attributes, "a");
    Assertions.assertTrue(val.isEmpty());
  }
}
