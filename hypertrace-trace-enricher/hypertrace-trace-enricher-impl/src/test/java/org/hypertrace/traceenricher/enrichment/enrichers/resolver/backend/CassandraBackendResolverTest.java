package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.BackendInfo;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.CassandraBackendResolver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CassandraBackendResolverTest {

  private CassandraBackendResolver cassandraBackendResolver;
  private StructuredTraceGraph structuredTraceGraph;

  @BeforeEach
  public void setup() {
    cassandraBackendResolver = new CassandraBackendResolver();
    structuredTraceGraph = mock(StructuredTraceGraph.class);
  }

  @Test
  public void testBackendResolution() {
    BackendInfo backendInfo =
        cassandraBackendResolver.resolve(getCassandraEvent(), structuredTraceGraph).get();
    Entity entity = backendInfo.getEntity();
    Assertions.assertEquals("localhost:9000", entity.getEntityName());
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(
        Map.of(
            "BACKEND_OPERATION",
            AttributeValueCreator.create("select"),
            "BACKEND_DESTINATION",
            AttributeValueCreator.create("customer.orders")),
        attributes);
  }

  @Test
  public void testBackendResolutionForOTEvent() {
    BackendInfo backendInfo =
        cassandraBackendResolver.resolve(getCassandraOTEvent(), structuredTraceGraph).get();
    Entity entity = backendInfo.getEntity();
    Assertions.assertEquals("localhost:9000", entity.getEntityName());
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(Map.of("BACKEND_OPERATION", AttributeValueCreator.create("select")), attributes);
  }

  @Test
  public void testBackendResolutionWithoutConnectionString() {
    BackendInfo backendInfo =
        cassandraBackendResolver.resolve(getCassandraOTelEvent(), structuredTraceGraph).get();
    Entity entity = backendInfo.getEntity();
    Assertions.assertEquals("test:9000", entity.getEntityName());
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(
        Map.of(
            "BACKEND_OPERATION",
            AttributeValueCreator.create("select"),
            "BACKEND_DESTINATION",
            AttributeValueCreator.create("customer.orders")),
        attributes);
  }

  private Event getCassandraEvent() {
    Event event =
        Event.newBuilder()
            .setCustomerId("__default")
            .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
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
                            "db.connection_string",
                            AttributeValue.newBuilder()
                                .setValue("jdbc:cassandra://localhost:9000")
                                .build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "db.system",
                            AttributeValue.newBuilder().setValue("cassandra").build(),
                            "db.statement",
                            AttributeValue.newBuilder()
                                .setValue("select * from audit_message")
                                .build(),
                            "db.operation",
                            AttributeValue.newBuilder().setValue("select").build(),
                            "db.cassandra.table",
                            AttributeValue.newBuilder().setValue("orders").build(),
                            "db.cassandra.keyspace",
                            AttributeValue.newBuilder().setValue("customer").build(),
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
            .setEventName("cassandra.connection")
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

  private Event getCassandraOTEvent() {
    Event event =
        Event.newBuilder()
            .setCustomerId("__default")
            .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
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
                            "peer.hostname",
                            AttributeValue.newBuilder().setValue("localhost").build(),
                            "peer.port",
                            AttributeValue.newBuilder().setValue("9000").build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "db.system",
                            AttributeValue.newBuilder().setValue("cassandra").build(),
                            "db.statement",
                            AttributeValue.newBuilder()
                                .setValue("select * from audit_message")
                                .build(),
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
            .setEventName("cassandra.connection")
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

  private Event getCassandraOTelEvent() {
    Event event =
        Event.newBuilder()
            .setCustomerId("__default")
            .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
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
                            "net.peer.name",
                            AttributeValue.newBuilder().setValue("test").build(),
                            "net.peer.port",
                            AttributeValue.newBuilder().setValue("9000").build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "db.system",
                            AttributeValue.newBuilder().setValue("cassandra").build(),
                            "db.statement",
                            AttributeValue.newBuilder()
                                .setValue("select * from audit_message")
                                .build(),
                            "db.cassandra.table",
                            AttributeValue.newBuilder().setValue("orders").build(),
                            "db.cassandra.keyspace",
                            AttributeValue.newBuilder().setValue("customer").build(),
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
            .setEventName("cassandra.connection")
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
