package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Mongo;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.client.EdsCacheClient;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BackendEntityEnricherTest extends AbstractAttributeEnricherTest {
  private static final String SERVICE_ID = "service1";
  private static final String EVENT_ID = "event1";
  private static final String API_BOUNDARY_TYPE_ATTR =
      EnrichedSpanConstants.getValue(Api.API_BOUNDARY_TYPE);
  private static final String SPAN_TYPE_ATTR =
      EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE);
  private static final String BACKEND_PROTOCOL_ATTR_NAME =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL);
  private static final String BACKEND_HOST_ATTR_NAME =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST);
  private static final String BACKEND_PORT_ATTR_NAME =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT);

  private BackendEntityEnricher enricher;
  private EntityCache entityCache;

  @Mock private EdsCacheClient edsClient;
  @Mock private ClientRegistry clientRegistry;

  @BeforeEach
  public void setup() {
    enricher = new BackendEntityEnricher();
    entityCache = new EntityCache(edsClient);
    when(clientRegistry.getEdsCacheClient()).thenReturn(edsClient);
    when(clientRegistry.getEntityCache()).thenReturn(entityCache);
    enricher.init(getEntityServiceConfig(), clientRegistry);
  }

  @Test
  public void test_EnrichTrace_apiEntryEvent() {
    Event e = createApiEntryEvent(EVENT_ID).build();
    StructuredTrace trace = createStructuredTrace(TENANT_ID, e);
    enricher.enrichTrace(trace);
    Assertions.assertEquals(ByteBuffer.wrap(EVENT_ID.getBytes()), e.getEventId());
    Assertions.assertNull(EnrichedSpanUtils.getBackendId(e));
  }

  @Test
  public void test_EnrichTrace_noBackendResolved() {
    Event e = createApiExitEvent(EVENT_ID).build();
    StructuredTrace trace = createStructuredTrace(TENANT_ID, e);
    enricher.enrichTrace(trace);
    Assertions.assertEquals(ByteBuffer.wrap(EVENT_ID.getBytes()), e.getEventId());
    Assertions.assertNull(EnrichedSpanUtils.getBackendId(e));
  }

  @Test
  public void test_EnrichTrace_ValidBackend() {
    String backendId = "backend1";
    String backendName = "mongo:27017";
    String eventName = "mongo exit";
    Map<String, String> identifyingAttributes =
        Map.of(
            BACKEND_PROTOCOL_ATTR_NAME, BackendType.MONGO.name(),
            BACKEND_HOST_ATTR_NAME, "mongo",
            BACKEND_PORT_ATTR_NAME, "27017");
    Map<String, String> attributes =
        Map.of(
            "FROM_EVENT",
            eventName,
            "FROM_EVENT_ID",
            HexUtils.getHex(ByteBuffer.wrap(EVENT_ID.getBytes())));
    Entity backendEntity =
        createEntity(EntityType.BACKEND, backendName, identifyingAttributes, attributes, TENANT_ID);

    when(edsClient.upsert(eq(backendEntity)))
        .thenReturn(
            Entity.newBuilder(backendEntity)
                .setEntityId(backendId)
                .putAllAttributes(createEdsAttributes(identifyingAttributes))
                .build());

    Event e = createApiExitEvent(EVENT_ID).setEventName("mongo exit").build();
    e.getAttributes()
        .getAttributeMap()
        .put(RawSpanConstants.getValue(Mongo.MONGO_URL), createAvroAttribute("mongo:27017"));
    StructuredTrace trace = createStructuredTrace(TENANT_ID, e);
    enricher.enrichTrace(trace);
    Assertions.assertEquals(ByteBuffer.wrap(EVENT_ID.getBytes()), e.getEventId());
    Assertions.assertEquals(backendId, EnrichedSpanUtils.getBackendId(e));
    Assertions.assertEquals(backendName, EnrichedSpanUtils.getBackendName(e));
  }

  private Event.Builder createApiEntryEvent(String eventId) {
    return Event.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEventId(ByteBuffer.wrap(eventId.getBytes()))
        .setEntityIdList(Collections.singletonList(SERVICE_ID))
        .setEnrichedAttributes(createNewAvroAttributes(Map.of(API_BOUNDARY_TYPE_ATTR, "ENTRY")))
        .setAttributes(createNewAvroAttributes());
  }

  private Event.Builder createApiExitEvent(String eventId) {
    return Event.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEventId(ByteBuffer.wrap(eventId.getBytes()))
        .setEntityIdList(Collections.singletonList(SERVICE_ID))
        .setEnrichedAttributes(createNewAvroAttributes(Map.of(API_BOUNDARY_TYPE_ATTR, "EXIT")))
        .setAttributes(createNewAvroAttributes(Map.of(SPAN_TYPE_ATTR, "EXIT")));
  }

  private org.hypertrace.entity.data.service.v1.Entity createEntity(
      EntityType entityType,
      String name,
      Map<String, String> identifyingAttributes,
      Map<String, String> attributes,
      String tenantId) {
    return org.hypertrace.entity.data.service.v1.Entity.newBuilder()
        .setEntityType(entityType.name())
        .setEntityName(name)
        .putAllIdentifyingAttributes(createEdsAttributes(identifyingAttributes))
        .putAllAttributes(createEdsAttributes(attributes))
        .setTenantId(tenantId)
        .build();
  }
}
