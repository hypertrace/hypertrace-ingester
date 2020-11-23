package org.hypertrace.telemetry.attribute.utils.messaging;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.telemetry.attribute.utils.SemanticConventionTestUtil;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.RabbitMq;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link OtelMessagingAttributes}
 */
public class OtelMessagingSemanticConventionUtilsTest {

  @Test
  public void testGetRabbitMqRoutingKey() {
    Event e = mock(Event.class);
    // otel format
    String routingKey = "otelRoutingKey";
    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(OtelMessagingAttributes.RABBITMQ_ROUTING_KEY.getValue(), SemanticConventionTestUtil.buildAttributeValue(routingKey)));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = MessagingSemanticConventionUtils.getRabbitMqRoutingKey(e);
    assertEquals(routingKey, v.get());

    // other format
    routingKey = "otherRoutingKey";
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY), SemanticConventionTestUtil
            .buildAttributeValue(routingKey)));
    when(e.getAttributes()).thenReturn(attributes);
    v = MessagingSemanticConventionUtils.getRabbitMqRoutingKey(e);
    assertEquals(routingKey, v.get());

    // routing key absent
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of("span.kind", SemanticConventionTestUtil.buildAttributeValue("client")));
    when(e.getAttributes()).thenReturn(attributes);
    v = MessagingSemanticConventionUtils.getRabbitMqRoutingKey(e);
    assertTrue(v.isEmpty());
  }
}