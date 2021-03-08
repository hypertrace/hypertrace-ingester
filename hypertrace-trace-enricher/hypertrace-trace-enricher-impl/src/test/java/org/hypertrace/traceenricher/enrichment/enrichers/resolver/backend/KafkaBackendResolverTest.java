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


/**
 * Unit Test for {@link KafkaBackendResolver}
 */
public class KafkaBackendResolverTest {

  private KafkaBackendResolver kafkaBackendResolver;
  private StructuredTraceGraph structuredTraceGraph;

  @BeforeEach
  public void setup() {
    kafkaBackendResolver = new KafkaBackendResolver();
    structuredTraceGraph = mock(StructuredTraceGraph.class);
  }

  @Test
  public void TestOtelBackendEventResolution() {
    String broker = "kafka-test.hypertrace.com:9092";
    Entity entity = kafkaBackendResolver.resolveEntity(getOtelKafkaBackendEvent(broker), structuredTraceGraph).get();
    Assertions.assertEquals(broker, entity.getEntityName());
  }

  @Test
  public void TestOTBackendEventResolution() {
    String brokerHost = "kafka-test.hypertrace.com";
    String brokerPort = "9092";
    Entity entity = kafkaBackendResolver.resolveEntity(getOTKafkaBackendEvent(brokerHost, brokerPort), structuredTraceGraph).get();
    Assertions.assertEquals(String.format("%s:%s", brokerHost, brokerPort), entity.getEntityName());
  }

  private Event getOtelKafkaBackendEvent(String broker) {
    Event event =  Event.newBuilder().setCustomerId("customer1")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f9".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("messaging.system", AttributeValue.newBuilder().setValue("kafka").build(),
                "messaging.url", AttributeValue.newBuilder().setValue(broker).build(),
                "span.kind", AttributeValue.newBuilder().setValue("client").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("0").build())).build())
        .setEventName("kafka.connection").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();


    return event;
  }

  private Event getOTKafkaBackendEvent(String host, String port) {
    Event event =  Event.newBuilder().setCustomerId("customer1")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f9".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("peer.service", AttributeValue.newBuilder().setValue("kafka").build(),
                "peer.hostname", AttributeValue.newBuilder().setValue(host).build(),
                "peer.port", AttributeValue.newBuilder().setValue(port).build(),
                "span.kind", AttributeValue.newBuilder().setValue("client").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("0").build())).build())
        .setEventName("kafka.connection").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();


    return event;
  }
}
