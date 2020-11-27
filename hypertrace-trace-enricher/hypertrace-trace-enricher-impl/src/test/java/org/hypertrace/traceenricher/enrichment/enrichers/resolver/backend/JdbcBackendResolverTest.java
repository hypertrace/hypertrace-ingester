package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.TestUtil.buildAttributeValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.span.constants.v1.Sql;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.constants.v1.K8sEntityAttribute;
import org.hypertrace.entity.data.service.client.EntityDataServiceClient;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.semantic.convention.utils.db.OTelDbSemanticConventions;
import org.hypertrace.semantic.convention.utils.span.OTelSpanSemanticConventions;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JdbcBackendResolver}
 */
public class JdbcBackendResolverTest {

  private JdbcBackendResolver jdbcBackendResolver;
  private StructuredTraceGraph structuredTraceGraph;

  @BeforeEach
  public void setup() {
    jdbcBackendResolver = new JdbcBackendResolver();
    structuredTraceGraph = mock(StructuredTraceGraph.class);
  }

  @Test
  public void testWebgoatUrl() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("sql.url", AttributeValue.newBuilder().setValue("jdbc:hsqldb:hsql://dbhost:9001/webgoat").build(),
                "span.kind", AttributeValue.newBuilder().setValue("client").build(), "sql.query",
                AttributeValue.newBuilder()
                    .setValue("insert into audit_message (message, id) values (?, ?)").build(),
                "k8s.pod_id",
                AttributeValue.newBuilder().setValue("55636196-c840-11e9-a417-42010a8a0064").build(),
                "docker.container_id", AttributeValue.newBuilder()
                    .setValue("ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("0").build())).build())
        .setEventName("jdbc.connection.prepare").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();
    final Entity backendEntity = jdbcBackendResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals("dbhost:9001", backendEntity.getEntityName());
    assertEquals(4, backendEntity.getIdentifyingAttributesCount());
    Assertions.assertEquals(BackendType.JDBC.name(),
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(
            BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString());
    assertEquals("dbhost",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString());
    assertEquals("9001",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString());
    assertEquals("hsqldb",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getRawSpanConstant(Sql.SQL_DB_TYPE)).getValue().getString());
    assertEquals("jdbc.connection.prepare",
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString());
    assertEquals("62646630336466616266356337306638",
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString());
    assertEquals("/webgoat",
        backendEntity.getAttributesMap().get(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH)).getValue().getString());
  }

  @Test
  public void testWithOtelFormatUrl() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map.of(
            OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
            buildAttributeValue("127.0.0.1"),
            OTelSpanSemanticConventions.NET_PEER_PORT.getValue(),
            buildAttributeValue("3306"),
            OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(), buildAttributeValue("service(localhost)\\db;;"),
            OTelDbSemanticConventions.DB_SYSTEM.getValue(), buildAttributeValue("mysql"),
            "span.kind", buildAttributeValue("client"),
            OTelDbSemanticConventions.DB_STATEMENT.getValue(), buildAttributeValue("SELECT * from example.user"),
            "k8s.pod_id", buildAttributeValue("55636196-c840-11e9-a417-42010a8a0064"),
            "docker.container_id", buildAttributeValue("ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f")
        )).build())
        .setEventName("jdbc.connection.prepare").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(
            Collections.singletonList(
                EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                    .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                    .setRefType(EventRefType.CHILD_OF).build())).build();

    Entity backendEntity = jdbcBackendResolver.resolveEntity(e, structuredTraceGraph).get();

    assertEquals("127.0.0.1:3306", backendEntity.getEntityName());
    assertEquals(BackendType.JDBC.name(),
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(
            BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString());
    assertEquals("127.0.0.1",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString());
    assertEquals("3306",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString());
    assertEquals("mysql",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getRawSpanConstant(Sql.SQL_DB_TYPE)).getValue().getString());
    assertEquals("jdbc.connection.prepare",
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString());
    assertEquals("62646630336466616266356337306638",
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString());
  }
}
