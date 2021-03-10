package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.data.service.v1.Entity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit test for {@link RabbitMqBackendResolver} */
public class RabbitMqBackendResolverTest {

  private RabbitMqBackendResolver rabbitMqBackendResolver;
  private StructuredTraceGraph structuredTraceGraph;

  @BeforeEach
  public void setup() {
    rabbitMqBackendResolver = new RabbitMqBackendResolver();
    structuredTraceGraph = mock(StructuredTraceGraph.class);
  }

  @Test
  public void testEventResolution() {
    String routingKey = "routingkey";
    Entity entity =
        rabbitMqBackendResolver
            .resolve(getRabbitMqEvent(routingKey), structuredTraceGraph)
            .get()
            .getEntity();
    Assertions.assertEquals(routingKey, entity.getEntityName());
  }

  private Event getRabbitMqEvent(String routingKey) {
    Event event =
        Event.newBuilder()
            .setCustomerId("customer1")
            .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f9".getBytes()))
            .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824"))
            .setEnrichedAttributes(
                Attributes.newBuilder()
                    .setAttributeMap(
                        Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build()))
                    .build())
            .setAttributes(
                Attributes.newBuilder()
                    .setAttributeMap(
                        Map.of(
                            "rabbitmq.routing_key",
                            AttributeValue.newBuilder().setValue(routingKey).build(),
                            "rabbitmq.message",
                            AttributeValue.newBuilder()
                                .setValue("updating user's last session")
                                .build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "k8s.pod_id",
                            AttributeValue.newBuilder()
                                .setValue("55636196-c840-11e9-a417-42010a8a0064")
                                .build(),
                            "docker.container_id",
                            AttributeValue.newBuilder()
                                .setValue(
                                    "ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f")
                                .build(),
                            "FLAGS",
                            AttributeValue.newBuilder().setValue("0").build()))
                    .build())
            .setEventName("rabbitmq.connection")
            .setStartTimeMillis(1566869077746L)
            .setEndTimeMillis(1566869077750L)
            .setMetrics(
                Metrics.newBuilder()
                    .setMetricMap(
                        Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build()))
                    .build())
            .setEventRefList(
                Arrays.asList(
                    EventRef.newBuilder()
                        .setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                        .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                        .setRefType(EventRefType.CHILD_OF)
                        .build()))
            .build();
    return event;
  }
}
