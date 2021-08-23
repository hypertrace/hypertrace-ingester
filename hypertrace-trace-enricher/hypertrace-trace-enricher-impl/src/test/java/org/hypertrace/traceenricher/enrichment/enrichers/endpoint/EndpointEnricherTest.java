package org.hypertrace.traceenricher.enrichment.enrichers.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.entity.constants.v1.ApiAttribute;
import org.hypertrace.entity.constants.v1.ServiceAttribute;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichment.enrichers.AbstractAttributeEnricherTest;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EndpointEnricherTest extends AbstractAttributeEnricherTest {

  private static final String API_NAME_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_NAME);
  private static final String API_DISCOVERY_STATE_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_DISCOVERY_STATE);

  private static final String TENANT_ID = "sometenant";
  private static final String SERVICE_ID = "service1";
  private static final String SERVICE_NAME = "someservice";
  private static final String API_ID = "api1";
  private static final String API_PATTERN_VAL = "Driver::getCustomers";
  private static final String API_NAME_VAL = "Driver::getCustomers";
  private static final String API_DISCOVERY_STATE_VAL = "DISCOVERED";
  private EndpointEnricher endpointEnricher;
  private ApiEntityDao dao;

  @BeforeEach
  void setup() {
    dao = mock(ApiEntityDao.class);
    endpointEnricher = new EndpointEnricher();
    endpointEnricher.setApiEntityDao(dao);
  }

  @Test
  void whenEnrichedAttributesAreMissing_thenNoEnrichmentWillHappen() {
    Event event =
        Event.newBuilder()
            .setCustomerId(TENANT_ID)
            .setEventId(ByteBuffer.wrap("name".getBytes()))
            .build();
    endpointEnricher.enrichEvent(getBigTrace(), event);
    String apiName = EnrichedSpanUtils.getApiName(event);
    assertNull(apiName);
  }

  @Test
  void whenServiceIdMissing_thenNoEnrichmentWillHappen() {
    Event event = createMockApiBoundaryEntryEvent();
    StructuredTrace trace = getBigTrace();
    endpointEnricher.enrichEvent(trace, event);
    String apiName = EnrichedSpanUtils.getApiName(event);
    assertNull(apiName);
  }

  @Test
  void whenAllAttributesArePresentThenExpectEventToBeEnriched() {
    StructuredTrace trace = getBigTrace();
    Event event = trace.getEventList().get(0);
    Entity entity =
        Entity.newBuilder()
            .setEntityId(API_ID)
            .setEntityName(API_PATTERN_VAL)
            .putAttributes(
                API_NAME_ATTR,
                org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString(API_NAME_VAL))
                    .build())
            .putAttributes(
                API_DISCOVERY_STATE_ATTR,
                org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString(API_DISCOVERY_STATE_VAL))
                    .build())
            .build();

    when(dao.upsertApiEntity(
            trace.getCustomerId(),
            SERVICE_ID,
            SERVICE_NAME,
            ApiEntityDao.API_TYPE,
            event.getEventName()))
        .thenReturn(entity);
    endpointEnricher.enrichEvent(trace, event);

    assertEquals(API_NAME_VAL, EnrichedSpanUtils.getApiName(event));
    assertEquals(API_PATTERN_VAL, EnrichedSpanUtils.getApiPattern(event));
    assertEquals(API_ID, EnrichedSpanUtils.getApiId(event));
    assertEquals(API_DISCOVERY_STATE_VAL, EnrichedSpanUtils.getApiDiscoveryState(event));
  }

  @Test
  void testApiEnrichmentForIntermediateEvents() {
    StructuredTrace trace = getBigTrace();
    Event exitEvent = trace.getEventList().get(2);
    endpointEnricher.enrichTrace(trace);

    assertEquals(API_NAME_VAL, EnrichedSpanUtils.getApiName(exitEvent));
    assertEquals(API_PATTERN_VAL, EnrichedSpanUtils.getApiPattern(exitEvent));
    assertEquals(API_ID, EnrichedSpanUtils.getApiId(exitEvent));

    Event intermediateEvent = trace.getEventList().get(1);
    assertEquals(API_NAME_VAL, EnrichedSpanUtils.getApiName(intermediateEvent));
    assertEquals(API_PATTERN_VAL, EnrichedSpanUtils.getApiPattern(intermediateEvent));
    assertEquals(API_ID, EnrichedSpanUtils.getApiId(intermediateEvent));
  }

  private Event createMockApiBoundaryEntryEvent() {
    Event e = createMockEvent();
    addEnrichedAttributeToEvent(
        e,
        Constants.getEnrichedSpanConstant(Api.API_BOUNDARY_TYPE),
        AttributeValueCreator.create(Constants.getEnrichedSpanConstant(ENTRY)));
    return e;
  }

  private Event createMockEvent() {
    Event e = mock(Event.class);
    when(e.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());
    when(e.getEnrichedAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());
    return e;
  }

  protected StructuredTrace getBigTrace() {
    /*
     * The graph looks like
     *             0 (entry)
     *             |
     *             1 (intermediate)
     * ------------|-------------------
     *             2 (exit)
     */
    // 0th raw span
    Map<String, AttributeValue> entrySpan0Map = new HashMap<>();
    entrySpan0Map.put(
        EnrichedSpanConstants.getValue(Http.HTTP_REQUEST_URL),
        AttributeValue.newBuilder().setValue("http://someservice.ai/users/1/checkout").build());
    Map<String, AttributeValue> enrichedEntrySpan0Map = new HashMap<>();
    enrichedEntrySpan0Map.put(
        EnrichedSpanConstants.getValue(Api.API_BOUNDARY_TYPE),
        AttributeValue.newBuilder().setValue("ENTRY").build());
    enrichedEntrySpan0Map.put(
        EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_ID),
        AttributeValue.newBuilder().setValue(SERVICE_ID).build());
    enrichedEntrySpan0Map.put(
        EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_NAME),
        AttributeValue.newBuilder().setValue(SERVICE_NAME).build());
    enrichedEntrySpan0Map.put(
        EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_ID),
        AttributeValue.newBuilder().setValue(API_ID).build());
    enrichedEntrySpan0Map.put(
        EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_URL_PATTERN),
        AttributeValue.newBuilder().setValue(API_PATTERN_VAL).build());
    enrichedEntrySpan0Map.put(
        EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_NAME),
        AttributeValue.newBuilder().setValue(API_NAME_VAL).build());
    Event event0 =
        Event.newBuilder()
            .setCustomerId(TENANT_ID)
            .setAttributes(Attributes.newBuilder().setAttributeMap(entrySpan0Map).build())
            .setEnrichedAttributes(
                Attributes.newBuilder().setAttributeMap(enrichedEntrySpan0Map).build())
            .setEventId(createByteBuffer("event0"))
            .setEventName(API_PATTERN_VAL)
            .build();
    RawSpan rawSpan0 =
        RawSpan.newBuilder()
            .setCustomerId(TENANT_ID)
            .setEvent(event0)
            .setTraceId(createByteBuffer("trace"))
            .build();

    // 1st intermediate raw span
    EventRef eventRef0 =
        EventRef.newBuilder()
            .setRefType(EventRefType.CHILD_OF)
            .setTraceId(createByteBuffer("trace"))
            .setEventId(createByteBuffer("event0"))
            .build();
    Map<String, AttributeValue> exitSpanMap = new HashMap<>();
    Map<String, AttributeValue> enrichedExitSpanMap = new HashMap<>();
    Event event1 =
        Event.newBuilder()
            .setCustomerId(TENANT_ID)
            .setEventId(createByteBuffer("event1"))
            .setAttributes(Attributes.newBuilder().setAttributeMap(exitSpanMap).build())
            .setEnrichedAttributes(
                Attributes.newBuilder().setAttributeMap(enrichedExitSpanMap).build())
            .setEventRefList(Collections.singletonList(eventRef0))
            .build();
    RawSpan rawSpan1 =
        RawSpan.newBuilder()
            .setCustomerId(TENANT_ID)
            .setEvent(event1)
            .setTraceId(createByteBuffer("trace"))
            .build();

    // 2nd exit raw span
    EventRef eventRef1 =
        EventRef.newBuilder()
            .setRefType(EventRefType.CHILD_OF)
            .setTraceId(createByteBuffer("trace"))
            .setEventId(createByteBuffer("event1"))
            .build();
    Map<String, AttributeValue> entrySpan2Map = new HashMap<>();
    entrySpan2Map.put(
        EnrichedSpanConstants.getValue(Http.HTTP_REQUEST_URL),
        AttributeValue.newBuilder().setValue("http://nextservice.ai/login").build());
    Map<String, AttributeValue> enrichedEntrySpan2Map = new HashMap<>();
    enrichedEntrySpan2Map.put(
        EnrichedSpanConstants.getValue(Api.API_BOUNDARY_TYPE),
        AttributeValue.newBuilder().setValue("EXIT").build());
    Event event2 =
        Event.newBuilder()
            .setCustomerId(TENANT_ID)
            .setAttributes(Attributes.newBuilder().setAttributeMap(entrySpan2Map).build())
            .setEnrichedAttributes(
                Attributes.newBuilder().setAttributeMap(enrichedEntrySpan2Map).build())
            .setEventId(createByteBuffer("event2"))
            .setEventRefList(Collections.singletonList(eventRef1))
            .build();
    RawSpan rawSpan2 =
        RawSpan.newBuilder()
            .setCustomerId(TENANT_ID)
            .setEvent(event2)
            .setTraceId(createByteBuffer("trace"))
            .build();

    return StructuredTraceBuilder.buildStructuredTraceFromRawSpans(
        List.of(rawSpan0, rawSpan1, rawSpan2), createByteBuffer("trace"), TENANT_ID);
  }

  private ByteBuffer createByteBuffer(String id) {
    return ByteBuffer.wrap(id.getBytes());
  }
}
