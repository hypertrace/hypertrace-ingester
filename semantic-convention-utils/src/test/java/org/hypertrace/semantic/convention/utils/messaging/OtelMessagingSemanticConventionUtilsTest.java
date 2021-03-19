package org.hypertrace.semantic.convention.utils.messaging;

import static org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil.buildAttributeValue;
import static org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil.buildAttributes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.semantic.convention.constants.messaging.OtelMessagingSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.RabbitMq;
import org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit test for {@link OtelMessagingSemanticConventions} */
public class OtelMessagingSemanticConventionUtilsTest {

  @Test
  public void testGetRabbitMqRoutingKey() {
    Event e = mock(Event.class);
    // otel format
    String routingKey = "otelRoutingKey";
    Attributes attributes =
        buildAttributes(
            Map.of(
                OtelMessagingSemanticConventions.RABBITMQ_ROUTING_KEY.getValue(),
                buildAttributeValue(routingKey)));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = MessagingSemanticConventionUtils.getRabbitMqRoutingKey(e);
    assertEquals(routingKey, v.get());

    // other format
    routingKey = "otherRoutingKey";
    attributes =
        buildAttributes(
            Map.of(
                RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY),
                buildAttributeValue(routingKey)));
    when(e.getAttributes()).thenReturn(attributes);
    v = MessagingSemanticConventionUtils.getRabbitMqRoutingKey(e);
    assertEquals(routingKey, v.get());

    // routing key absent
    attributes = buildAttributes(Map.of("span.kind", buildAttributeValue("client")));
    when(e.getAttributes()).thenReturn(attributes);
    v = MessagingSemanticConventionUtils.getRabbitMqRoutingKey(e);
    assertTrue(v.isEmpty());
  }

  @Test
  public void testIsRabbitMqBackend() {
    Event e = mock(Event.class);
    // otel format
    String routingKey = "otelRoutingKey";
    Attributes attributes =
        buildAttributes(
            Map.of(
                OtelMessagingSemanticConventions.RABBITMQ_ROUTING_KEY.getValue(),
                buildAttributeValue(routingKey)));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = MessagingSemanticConventionUtils.isRabbitMqBackend(e);
    assertTrue(v);
    // other format
    routingKey = "otherRoutingKey";
    attributes =
        buildAttributes(
            Map.of(
                RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY),
                buildAttributeValue(routingKey)));
    when(e.getAttributes()).thenReturn(attributes);
    v = MessagingSemanticConventionUtils.isRabbitMqBackend(e);
    assertTrue(v);
    // not present
    attributes = buildAttributes(Map.of());
    when(e.getAttributes()).thenReturn(attributes);
    v = MessagingSemanticConventionUtils.isRabbitMqBackend(e);
    Assertions.assertFalse(v);
  }

  @Test
  public void testGetRabbitmqDestination() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OtelMessagingSemanticConventions.MESSAGING_DESTINATION.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("queueName"),
                OtelMessagingSemanticConventions.RABBITMQ_ROUTING_KEY.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("test-key")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = MessagingSemanticConventionUtils.getMessagingDestinationForRabbitmq(e);
    assertEquals("test-key.queueName", v.get());
  }

  @Test
  public void testGetRabbitmqOperation() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OtelMessagingSemanticConventions.MESSAGING_OPERATION.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("publish")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = MessagingSemanticConventionUtils.getMessagingOperation(e);
    assertEquals("publish", v.get());
  }

  @Test
  public void testIfRabbitmqOperationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(MessagingSemanticConventionUtils.getMessagingOperation(e).isEmpty());
  }

  @Test
  public void testIfRabbitmqDestinationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(MessagingSemanticConventionUtils.getMessagingDestinationForRabbitmq(e).isEmpty());
  }


  @Test
  public void testGetKafkaDestination() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OtelMessagingSemanticConventions.MESSAGING_DESTINATION.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("queueName"),
                OtelMessagingSemanticConventions.MESSAGING_KAFKA_CONSUMER_GROUP.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("test")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = MessagingSemanticConventionUtils.getMessagingDestinationForKafka(e);
    assertEquals("test.queueName", v.get());
  }

  @Test
  public void testGetKafkaOperation() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OtelMessagingSemanticConventions.MESSAGING_OPERATION.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("publish")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = MessagingSemanticConventionUtils.getMessagingOperation(e);
    assertEquals("publish", v.get());
  }

  @Test
  public void testIfKafkaOperationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(MessagingSemanticConventionUtils.getMessagingOperation(e).isEmpty());
  }

  @Test
  public void testIfKafkaDestinationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(MessagingSemanticConventionUtils.getMessagingDestinationForKafka(e).isEmpty());
  }


  @Test
  public void testGetSqsDestination() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OtelMessagingSemanticConventions.MESSAGING_DESTINATION.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("queueName")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = MessagingSemanticConventionUtils.getMessagingDestination(e);
    assertEquals("queueName", v.get());
  }

  @Test
  public void testGetSqsOperation() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OtelMessagingSemanticConventions.MESSAGING_OPERATION.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("publish")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = MessagingSemanticConventionUtils.getMessagingOperation(e);
    assertEquals("publish", v.get());
  }

  @Test
  public void testIfSqsOperationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(MessagingSemanticConventionUtils.getMessagingOperation(e).isEmpty());
  }

  @Test
  public void testIfSqsDestinationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(MessagingSemanticConventionUtils.getMessagingDestination(e).isEmpty());
  }

}
