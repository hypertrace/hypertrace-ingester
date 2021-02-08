package org.hypertrace.semantic.convention.utils.messaging;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.semantic.convention.constants.messaging.OtelMessagingSemanticConventions;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.RabbitMq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil.buildAttributeValue;
import static org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil.buildAttributes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link OtelMessagingSemanticConventions}
 */
public class OtelMessagingSemanticConventionUtilsTest {

  @Test
  public void testGetRabbitMqRoutingKey() {
    Event e = mock(Event.class);
    // otel format
    String routingKey = "otelRoutingKey";
    Attributes attributes = buildAttributes(
        Map.of(OtelMessagingSemanticConventions.RABBITMQ_ROUTING_KEY.getValue(), buildAttributeValue(routingKey)));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = MessagingSemanticConventionUtils.getRabbitMqRoutingKey(e);
    assertEquals(routingKey, v.get());

    // other format
    routingKey = "otherRoutingKey";
    attributes = buildAttributes(
        Map.of(RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY), buildAttributeValue(routingKey)));
    when(e.getAttributes()).thenReturn(attributes);
    v = MessagingSemanticConventionUtils.getRabbitMqRoutingKey(e);
    assertEquals(routingKey, v.get());

    // routing key absent
    attributes = buildAttributes(
        Map.of("span.kind", buildAttributeValue("client")));
    when(e.getAttributes()).thenReturn(attributes);
    v = MessagingSemanticConventionUtils.getRabbitMqRoutingKey(e);
    assertTrue(v.isEmpty());
  }

  @Test
  public void testIsRabbitMqBackend() {
    Event e = mock(Event.class);
    // otel format
    String routingKey = "otelRoutingKey";
    Attributes attributes = buildAttributes(
        Map.of(OtelMessagingSemanticConventions.RABBITMQ_ROUTING_KEY.getValue(), buildAttributeValue(routingKey)));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = MessagingSemanticConventionUtils.isRabbitMqBackend(e);
    assertTrue(v);
    // other format
    routingKey = "otherRoutingKey";
    attributes = buildAttributes(
        Map.of(RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY), buildAttributeValue(routingKey)));
    when(e.getAttributes()).thenReturn(attributes);
    v = MessagingSemanticConventionUtils.isRabbitMqBackend(e);
    assertTrue(v);
    // not present
    attributes = buildAttributes(Map.of());
    when(e.getAttributes()).thenReturn(attributes);
    v = MessagingSemanticConventionUtils.isRabbitMqBackend(e);
    Assertions.assertFalse(v);
  }
}