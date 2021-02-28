package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.data.service.client.EdsCacheClient;
import org.hypertrace.entity.data.service.client.EntityDataServiceClient;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DefaultServiceEntityEnricherTest extends AbstractAttributeEnricherTest {

  private static final String ENTITY_ID = "entity1";

  private static final String API_BOUNDARY_TYPE_ATTR =
      EnrichedSpanConstants.getValue(Api.API_BOUNDARY_TYPE);

  private DefaultServiceEntityEnricher enricher;

  @Mock
  private EdsCacheClient edsClient;
  @Mock
  private ClientRegistry clientRegistry;
  private EntityCache entityCache;

  @BeforeEach
  public void setup() {
    enricher = new DefaultServiceEntityEnricher();
    entityCache = new EntityCache(this.edsClient);
    when(clientRegistry.getEdsCacheClient())
        .thenReturn(edsClient);
    when(clientRegistry.getEntityCache())
        .thenReturn(entityCache);

    enricher.init(getEntityServiceConfig(), clientRegistry);
  }

  @Test
  public void test_enrichTrace_opensourceSpan() {
    String serviceName = "testService";
    org.hypertrace.entity.data.service.v1.Entity service = org.hypertrace.entity.data.service.v1.Entity.newBuilder()
        .setTenantId(TENANT_ID).setEntityType(EntityType.SERVICE.name())
        .setEntityId(ENTITY_ID).setEntityName(serviceName).build();
    doReturn(Collections.singletonList(service))
        .when(edsClient).getEntitiesByName(TENANT_ID, EntityType.SERVICE.name(), serviceName);

    Event e1 = createOpenSourceSpan(TENANT_ID, "event1", serviceName, "ENTRY").build();
    Event e2 = createOpenSourceSpan(TENANT_ID, "event2", serviceName, "ENTRY").build();
    StructuredTrace trace = createStructuredTrace(TENANT_ID, e1, e2);
    enricher.enrichEvent(trace, e1);
    enricher.enrichEvent(trace, e2);

    // Assert that the entity is added on both the trace and event.
    assertEquals(1, trace.getEntityList().size());
    assertEquals(1, e1.getEntityIdList().size());
    assertEquals(1, e2.getEntityIdList().size());
    assertEquals(serviceName, EnrichedSpanUtils.getServiceName(e1));
    assertEquals(serviceName, EnrichedSpanUtils.getServiceName(e2));
    assertEquals(ENTITY_ID, EnrichedSpanUtils.getServiceId(e1));
    assertEquals(ENTITY_ID, EnrichedSpanUtils.getServiceId(e2));
  }

  @Test
  public void test_enrichTrace_spanNoContainerInfo() {
    String serviceName = "testService";
    org.hypertrace.entity.data.service.v1.Entity service = org.hypertrace.entity.data.service.v1.Entity.newBuilder()
        .setTenantId(TENANT_ID).setEntityType(EntityType.SERVICE.name())
        .setEntityId(ENTITY_ID).setEntityName(serviceName).build();
    doReturn(Collections.singletonList(service))
        .when(edsClient).getEntitiesByName(TENANT_ID, EntityType.SERVICE.name(), serviceName);

    Event e1 = createOpenSourceSpan(TENANT_ID, "event1", serviceName, "ENTRY").build();
    StructuredTrace trace = createStructuredTrace(TENANT_ID, e1);
    enricher.enrichEvent(trace, e1);

    // Assert that the entity is added on both the trace and event.
    assertEquals(1, trace.getEntityList().size());
    assertEquals(1, e1.getEntityIdList().size());
    assertEquals(serviceName, EnrichedSpanUtils.getServiceName(e1));
    assertEquals(ENTITY_ID, EnrichedSpanUtils.getServiceId(e1));
  }

  @Test
  public void testEnrichTraceSpanNoContainerInfoExitSpan() {
    String serviceName = "testService";
    org.hypertrace.entity.data.service.v1.Entity service = org.hypertrace.entity.data.service.v1.Entity.newBuilder()
        .setTenantId(TENANT_ID).setEntityType(EntityType.SERVICE.name())
        .setEntityId(ENTITY_ID).setEntityName(serviceName).build();
    doReturn(Collections.singletonList(service))
        .when(edsClient).getEntitiesByName(TENANT_ID, EntityType.SERVICE.name(), serviceName);

    Event e1 = createOpenSourceSpan(TENANT_ID, "event1", serviceName, "EXIT").build();
    StructuredTrace trace = createStructuredTrace(TENANT_ID, e1);
    enricher.enrichEvent(trace, e1);

    // Assert that the entity is added on both the trace and event.
    assertEquals(1, trace.getEntityList().size());
    assertEquals(1, e1.getEntityIdList().size());
    assertEquals(serviceName, EnrichedSpanUtils.getServiceName(e1));
    assertEquals(ENTITY_ID, EnrichedSpanUtils.getServiceId(e1));
  }

  @Test
  public void testEnrichTraceNoServiceName() {
    org.hypertrace.entity.data.service.v1.Entity service = org.hypertrace.entity.data.service.v1.Entity.newBuilder()
        .setTenantId(TENANT_ID).setEntityType(EntityType.SERVICE.name())
        .setEntityId(ENTITY_ID).build();
    Event e1 = createOpenSourceSpan(TENANT_ID, "event1",  null, "ENTRY").build();
    StructuredTrace trace = createStructuredTrace(TENANT_ID, e1);
    enricher.enrichEvent(trace, e1);
    // Assert that the entity is not added on both the trace and event as serviceName is null
    assertEquals(0, trace.getEntityList().size());
    assertEquals(0, e1.getEntityIdList().size());
    assertNull(EnrichedSpanUtils.getServiceName(e1));
  }


  @Test
  public void test_case1_findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService() {
    // Case 1:  sampleapp (entry) -> redis (exit)
    Event parent = createEvent("parent",
        Map.of("span.kind", "server"),
        Map.of("SPAN_TYPE", "ENTRY", "SERVICE_NAME", "sampleapp"), null, "sampleapp");
    Event current = createEvent("current",
        Map.of("span.kind", "client"),
        Map.of("SPAN_TYPE", "EXIT"), ByteBuffer.wrap("parent".getBytes()), "redis");

    StructuredTraceGraph graph = mock(StructuredTraceGraph.class);
    when(graph.getParentEvent(current)).thenReturn(parent);
    String actual = enricher
        .findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService(current,
            "redis", graph).orElse(null);
    String expected = "sampleapp";
    assertEquals(expected, actual);
  }

  @Test
  public void test_case2_findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService() {
    // Case 2:  sampleapp (entry) -> sampleapp (exit)
    Event parent = createEvent("parent",
        Map.of("span.kind", "server"),
        Map.of("SPAN_TYPE", "ENTRY", "SERVICE_NAME", "sampleapp"), null, "sampleapp");
    Event current = createEvent("current",
        Map.of("span.kind", "client"),
        Map.of("SPAN_TYPE", "EXIT"), ByteBuffer.wrap("parent".getBytes()), "sampleapp");

    StructuredTraceGraph graph = mock(StructuredTraceGraph.class);
    when(graph.getParentEvent(current)).thenReturn(parent);
    String actual = enricher
        .findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService(current,
            "sampleapp", graph).orElse(null);
    String expected = null;
    assertEquals(expected, actual);
  }

  @Test
  public void test_case3_findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService() {
    // Case 3:  sampleapp (entry) -> sampleapp (exit) -> redis (exit)
    Event parent1 = createEvent("parent1",
        Map.of("span.kind", "server"),
        Map.of(API_BOUNDARY_TYPE_ATTR, "ENTRY", "SERVICE_NAME", "sampleapp"), null, "sampleapp");

    Event parent2 = createEvent("parent2",
        Map.of("span.kind", "client"),
        Map.of(API_BOUNDARY_TYPE_ATTR, "EXIT", "SERVICE_NAME", "sampleapp"),
        ByteBuffer.wrap("parent1".getBytes()), "sampleapp");

    Event current = createEvent("current",
        Map.of("span.kind", "client"),
        Map.of(API_BOUNDARY_TYPE_ATTR, "EXIT"), ByteBuffer.wrap("parent2".getBytes()), "redis");

    StructuredTraceGraph graph = mock(StructuredTraceGraph.class);
    when(graph.getParentEvent(current)).thenReturn(parent2);
    when(graph.getParentEvent(parent2)).thenReturn(parent1);
    String actual = enricher
        .findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService(current,
            "redis", graph).orElse(null);
    String expected = "sampleapp";
    assertEquals(expected, actual);
  }

  @Test
  public void test_case4_findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService() {
    // Case 4:  sampleapp (entry) -> sampleapp (exit) -> sampleapp (exit)
    Event parent1 = createEvent("parent1",
        Map.of("span.kind", "server"),
        Map.of(API_BOUNDARY_TYPE_ATTR, "ENTRY", "SERVICE_NAME", "sampleapp"), null, "sampleapp");
    Event parent2 = createEvent("parent2",
        Map.of("span.kind", "client"),
        Map.of(API_BOUNDARY_TYPE_ATTR, "EXIT", "SERVICE_NAME", "sampleapp"),
        ByteBuffer.wrap("parent1".getBytes()), "sampleapp");
    Event current = createEvent("current",
        Map.of("span.kind", "client"),
        Map.of(API_BOUNDARY_TYPE_ATTR, "EXIT"), ByteBuffer.wrap("parent2".getBytes()), "sampleapp");

    StructuredTraceGraph graph = mock(StructuredTraceGraph.class);
    when(graph.getParentEvent(current)).thenReturn(parent2);
    String actual = enricher
        .findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService(current,
            "sampleapp", graph).orElse(null);
    String expected = null;
    assertEquals(expected, actual);
  }

  @Test
  public void test_case5_findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService() {
    // Case 4:  sampleapp (exit)
    Event current = createEvent("current",
        Map.of("span.kind", "client"),
        Map.of(API_BOUNDARY_TYPE_ATTR, "EXIT"), null, "sampleapp");

    StructuredTraceGraph graph = mock(StructuredTraceGraph.class);
    when(graph.getParentEvent(current)).thenReturn(null);

    String actual = enricher
        .findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService(current,
            "sampleapp", graph).orElse(null);
    String expected = null;
    assertEquals(expected, actual);
  }

  @Test
  public void test_case6_findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService() {
    // Case 6:  sampleapp (entry) -> sampleapp (entry) -> sampleapp (exit)
    Event parent1 = createEvent("parent1",
        Map.of("span.kind", "server"),
        Map.of(API_BOUNDARY_TYPE_ATTR, "ENTRY", "SERVICE_NAME", "sampleapp"), null, "sampleapp");
    Event parent2 = createEvent("parent2",
        Map.of("span.kind", "client"),
        Map.of(API_BOUNDARY_TYPE_ATTR, "ENTRY", "SERVICE_NAME", "sampleapp"),
        ByteBuffer.wrap("parent1".getBytes()), "sampleapp");
    Event current = createEvent("current",
        Map.of("span.kind", "client"),
        Map.of(API_BOUNDARY_TYPE_ATTR, "EXIT"), ByteBuffer.wrap("parent2".getBytes()), "sampleapp");

    StructuredTraceGraph graph = mock(StructuredTraceGraph.class);
    when(graph.getParentEvent(current)).thenReturn(parent2);
    String actual = enricher
        .findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService(current,
            "sampleapp", graph).orElse(null);
    String expected = null;
    assertEquals(expected, actual);
  }


  private Event createEvent(String eventName, Map<String, String> attributes,
                            Map<String, String> enriched, ByteBuffer parentEventId, String serviceName) {

    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributes.forEach(
        (s, s2) -> attributeMap.put(s, AttributeValue.newBuilder().setValue(s2).build()));

    Map<String, AttributeValue> enrichedAttributesMap = new HashMap<>();
    enriched.forEach(
        (s, s2) -> enrichedAttributesMap.put(s, AttributeValue.newBuilder().setValue(s2).build()));

    List<EventRef> eventRefList = new ArrayList<>();
    if (parentEventId != null) {
      eventRefList
          .add(EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
              .setEventId(parentEventId)
              .setRefType(EventRefType.CHILD_OF).build());
    }

    return Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap(eventName.getBytes()))
        .setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(enrichedAttributesMap).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(attributeMap).build())
        .setEventName(eventName)
        .setServiceName(serviceName)
        .setEventRefList(eventRefList).build();
  }

}
