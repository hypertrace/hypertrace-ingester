package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import static org.hypertrace.traceenricher.TestUtil.buildAttributeValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.semantic.convention.constants.db.OTelDbSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.v1.Sql;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichedspan.constants.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.AbstractBackendEntityEnricher;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.FqnResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.HypertraceFqnResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.BackendInfo;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link JdbcBackendProvider} */
public class JdbcBackendProviderTest {
  private AbstractBackendEntityEnricher backendEntityEnricher;
  private StructuredTraceGraph structuredTraceGraph;
  private StructuredTrace structuredTrace;

  @BeforeEach
  public void setup() {
    backendEntityEnricher = new MockBackendEntityEnricher();
    backendEntityEnricher.init(ConfigFactory.empty(), mock(ClientRegistry.class));

    structuredTrace = mock(StructuredTrace.class);
    structuredTraceGraph = mock(StructuredTraceGraph.class);
  }

  @Test
  public void testWebgoatUrl() {
    Event e =
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
                            "sql.url",
                            AttributeValue.newBuilder()
                                .setValue("jdbc:hsqldb:hsql://dbhost:9001/webgoat")
                                .build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "sql.query",
                            AttributeValue.newBuilder()
                                .setValue("insert into audit_message (message, id) values (?, ?)")
                                .build(),
                            "db.operation",
                            AttributeValue.newBuilder().setValue("select").build(),
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
            .setEventName("jdbc.connection.prepare")
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
    BackendInfo backendInfo =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get();
    final Entity backendEntity = backendInfo.getEntity();
    assertEquals("dbhost:9001", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    Assertions.assertEquals(
        BackendType.JDBC.name(),
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue()
            .getString());
    assertEquals(
        "dbhost",
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST))
            .getValue()
            .getString());
    assertEquals(
        "9001",
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT))
            .getValue()
            .getString());
    assertEquals(
        "hsqldb",
        backendEntity
            .getAttributesMap()
            .get(Constants.getRawSpanConstant(Sql.SQL_DB_TYPE))
            .getValue()
            .getString());
    assertEquals(
        "jdbc.connection.prepare",
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT))
            .getValue()
            .getString());
    assertEquals(
        "62646630336466616266356337306638",
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID))
            .getValue()
            .getString());
    assertEquals(
        "/webgoat",
        backendEntity
            .getAttributesMap()
            .get(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH))
            .getValue()
            .getString());

    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(Map.of("BACKEND_OPERATION", AttributeValueCreator.create("select")), attributes);
  }

  @Test
  public void testBackendOperationWithSqlQuery() {
    Event e =
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
                            "sql.url",
                            AttributeValue.newBuilder()
                                .setValue("jdbc:hsqldb:hsql://dbhost:9001/webgoat")
                                .build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "sql.query",
                            AttributeValue.newBuilder()
                                .setValue("insert into audit_message (message, id) values (?, ?)")
                                .build()))
                    .build())
            .setEventName("jdbc.connection.prepare")
            .setEventRefList(
                Arrays.asList(
                    EventRef.newBuilder()
                        .setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                        .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                        .setRefType(EventRefType.CHILD_OF)
                        .build()))
            .build();
    BackendInfo backendInfo =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get();
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(Map.of("BACKEND_OPERATION", AttributeValueCreator.create("insert")), attributes);
  }

  @Test
  public void testBackendOperationWithDbStatement() {
    Event e =
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
                            "sql.url",
                            AttributeValue.newBuilder()
                                .setValue("jdbc:hsqldb:hsql://dbhost:9001/webgoat")
                                .build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "db.statement",
                            AttributeValue.newBuilder()
                                .setValue("insert into audit_message (message, id) values (?, ?)")
                                .build()))
                    .build())
            .setEventName("jdbc.connection.prepare")
            .setEventRefList(
                Arrays.asList(
                    EventRef.newBuilder()
                        .setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                        .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                        .setRefType(EventRefType.CHILD_OF)
                        .build()))
            .build();
    BackendInfo backendInfo =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get();
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(Map.of("BACKEND_OPERATION", AttributeValueCreator.create("insert")), attributes);
  }

  @Test
  public void testBackendDestinationWithTableName() {
    Event e =
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
                            "sql.url",
                            AttributeValue.newBuilder()
                                .setValue("jdbc:hsqldb:hsql://dbhost:9001/webgoat")
                                .build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "db.sql.table",
                            AttributeValue.newBuilder().setValue("orders").build(),
                            "db.name",
                            AttributeValue.newBuilder().setValue("customer").build()))
                    .build())
            .setEventName("jdbc.connection.prepare")
            .setEventRefList(
                Arrays.asList(
                    EventRef.newBuilder()
                        .setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                        .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                        .setRefType(EventRefType.CHILD_OF)
                        .build()))
            .build();
    BackendInfo backendInfo =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get();
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(
        Map.of("BACKEND_DESTINATION", AttributeValueCreator.create("customer.orders")), attributes);
  }

  @Test
  public void testBackendDestinationWithoutTableName() {
    Event e =
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
                            "sql.url",
                            AttributeValue.newBuilder()
                                .setValue("jdbc:hsqldb:hsql://dbhost:9001/webgoat")
                                .build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "db.name",
                            AttributeValue.newBuilder().setValue("customer").build()))
                    .build())
            .setEventName("jdbc.connection.prepare")
            .setEventRefList(
                Arrays.asList(
                    EventRef.newBuilder()
                        .setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                        .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                        .setRefType(EventRefType.CHILD_OF)
                        .build()))
            .build();
    BackendInfo backendInfo =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get();
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(
        Map.of("BACKEND_DESTINATION", AttributeValueCreator.create("customer")), attributes);
  }

  @Test
  public void testWithOtelFormatUrl() {
    Event e =
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
                            OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
                            buildAttributeValue("127.0.0.1"),
                            OTelSpanSemanticConventions.NET_PEER_PORT.getValue(),
                            buildAttributeValue("3306"),
                            OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(),
                            buildAttributeValue("service(localhost)\\db;;"),
                            OTelDbSemanticConventions.DB_SYSTEM.getValue(),
                            buildAttributeValue("mysql"),
                            "span.kind",
                            buildAttributeValue("client"),
                            OTelDbSemanticConventions.DB_STATEMENT.getValue(),
                            buildAttributeValue("SELECT * from example.user"),
                            "k8s.pod_id",
                            buildAttributeValue("55636196-c840-11e9-a417-42010a8a0064"),
                            "docker.container_id",
                            buildAttributeValue(
                                "ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f")))
                    .build())
            .setEventName("jdbc.connection.prepare")
            .setStartTimeMillis(1566869077746L)
            .setEndTimeMillis(1566869077750L)
            .setMetrics(
                Metrics.newBuilder()
                    .setMetricMap(
                        Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build()))
                    .build())
            .setEventRefList(
                Collections.singletonList(
                    EventRef.newBuilder()
                        .setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                        .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                        .setRefType(EventRefType.CHILD_OF)
                        .build()))
            .build();

    BackendInfo backendInfo =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get();
    final Entity backendEntity = backendInfo.getEntity();

    assertEquals("127.0.0.1:3306", backendEntity.getEntityName());
    assertEquals(
        BackendType.JDBC.name(),
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue()
            .getString());
    assertEquals(
        "127.0.0.1",
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST))
            .getValue()
            .getString());
    assertEquals(
        "3306",
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT))
            .getValue()
            .getString());
    assertEquals(
        "mysql",
        backendEntity
            .getAttributesMap()
            .get(Constants.getRawSpanConstant(Sql.SQL_DB_TYPE))
            .getValue()
            .getString());
    assertEquals(
        "jdbc.connection.prepare",
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT))
            .getValue()
            .getString());
    assertEquals(
        "62646630336466616266356337306638",
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID))
            .getValue()
            .getString());

    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(Map.of("BACKEND_OPERATION", AttributeValueCreator.create("SELECT")), attributes);
  }

  @Test
  public void checkBackendEntityGeneratedFromJdbcEvent() {
    Event e =
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
                            "sql.url",
                            AttributeValue.newBuilder()
                                .setValue("jdbc:mysql://mysql:3306/shop")
                                .build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "sql.query",
                            AttributeValue.newBuilder()
                                .setValue("insert into audit_message (message, id) values (?, ?)")
                                .build(),
                            "k8s.pod_id",
                            AttributeValue.newBuilder()
                                .setValue("55636196-c840-11e9-a417-42010a8a0064")
                                .build(),
                            "db.operation",
                            AttributeValue.newBuilder().setValue("select").build(),
                            "docker.container_id",
                            AttributeValue.newBuilder()
                                .setValue(
                                    "ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f")
                                .build(),
                            "FLAGS",
                            AttributeValue.newBuilder().setValue("0").build()))
                    .build())
            .setEventName("jdbc.connection.prepare")
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
    BackendInfo backendInfo =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get();
    final Entity backendEntity = backendInfo.getEntity();
    assertEquals("mysql:3306", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    Assertions.assertEquals(
        BackendType.JDBC.name(),
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue()
            .getString());
    assertEquals(
        "mysql",
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST))
            .getValue()
            .getString());
    assertEquals(
        "3306",
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT))
            .getValue()
            .getString());
    assertEquals(
        "mysql",
        backendEntity
            .getAttributesMap()
            .get(Constants.getRawSpanConstant(Sql.SQL_DB_TYPE))
            .getValue()
            .getString());
    assertEquals(
        "jdbc.connection.prepare",
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT))
            .getValue()
            .getString());
    assertEquals(
        "62646630336466616266356337306638",
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID))
            .getValue()
            .getString());

    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(Map.of("BACKEND_OPERATION", AttributeValueCreator.create("select")), attributes);
  }

  @Test
  public void testGetBackendEntity() {
    Event e =
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
                            "sql.url",
                            AttributeValue.newBuilder()
                                .setValue("jdbc:mysql://mysql:3306/shop")
                                .build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "sql.query",
                            AttributeValue.newBuilder()
                                .setValue("insert into audit_message (message, id) values (?, ?)")
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
            .setEventName("jdbc.connection.prepare")
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
    Entity backendEntity =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get().getEntity();
    Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> idAttrMap =
        backendEntity.getIdentifyingAttributesMap();
    assertEquals(
        "mysql",
        idAttrMap
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST))
            .getValue()
            .getString());
    assertEquals(
        "3306",
        idAttrMap
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT))
            .getValue()
            .getString());
    assertEquals(
        "JDBC",
        idAttrMap
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue()
            .getString());
    assertEquals(
        "mysql",
        backendEntity
            .getAttributesMap()
            .get(Constants.getRawSpanConstant(Sql.SQL_DB_TYPE))
            .getValue()
            .getString());
  }

  static class MockBackendEntityEnricher extends AbstractBackendEntityEnricher {

    @Override
    public void setup(Config enricherConfig, ClientRegistry clientRegistry) {}

    @Override
    public List<BackendProvider> getBackendProviders() {
      return List.of(new JdbcBackendProvider());
    }

    @Override
    public FqnResolver getFqnResolver() {
      return new HypertraceFqnResolver();
    }
  }
}
