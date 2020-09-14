package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.hypertrace.core.span.constants.v1.Docker;
import org.hypertrace.core.span.constants.v1.Envoy;
import org.hypertrace.core.span.constants.v1.TracerAttribute;
import org.hypertrace.entity.constants.v1.ServiceAttribute;
import org.hypertrace.entity.data.service.client.EntityDataServiceClient;
import org.hypertrace.entity.data.service.client.EntityDataServiceClientProvider;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.util.Constants;

public class AbstractAttributeEnricherTest {
  protected static final String TENANT_ID = "tenant1";

  protected static final CommonAttribute SPAN_TYPE = CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE;
  protected static final BoundaryTypeValue ENTRY = BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY;
  protected static final BoundaryTypeValue EXIT = BoundaryTypeValue.BOUNDARY_TYPE_VALUE_EXIT;
  protected static final BoundaryTypeValue UNKNOWN = BoundaryTypeValue.BOUNDARY_TYPE_VALUE_UNSPECIFIED;

  private static final TracerAttribute TRACER_TYPE = TracerAttribute.TRACER_ATTRIBUTE_TRACER_TYPE;
  private static final String PROXY_TRACER = "proxy";
  private static final String JAVA_TRACER = "java";

  static final String COMMON_ENTITY_ID = "service1";
  static final String COMMON_ENTITY_NAME = "entityName1";

  protected Config getEntityServiceConfig() {
    Map<String, Object> entityServiceConfig = new HashMap<>();
    entityServiceConfig.put("host", "localhost");
    entityServiceConfig.put("port", 50061);
    Map<String, Object> map = new HashMap<>();
    map.put("entity.service.config", entityServiceConfig);
    return ConfigFactory.parseMap(map);
  }

  EntityDataServiceClientProvider createProvider(EntityDataServiceClient client) {
    return config -> client;
  }

  Event createMockEvent() {
    Event e = mock(Event.class);
    when(e.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());
    when(e.getEnrichedAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());
    lenient().when(e.getMetrics())
        .thenReturn(Metrics.newBuilder().setMetricMap(new HashMap<>()).build());
    return e;
  }

  Event createMockEnvoyEvent(String operationName) {
    Event e = createMockEvent();
    if (operationName != null) {
      addAttributeToEvent(e, Constants.getRawSpanConstant(Envoy.ENVOY_OPERATION_NAME),
          AttributeValueCreator.create(operationName));
    }
    return e;
  }

  Event createMockExitEvent() {
    Event e = createMockEvent();
    addEnrichedAttributeToEvent(e, Constants.getEnrichedSpanConstant(SPAN_TYPE),
        AttributeValueCreator.create(Constants.getEnrichedSpanConstant(EXIT)));
    return e;
  }

  Event createMockEntryEvent() {
    Event e = createMockEvent();
    addEnrichedAttributeToEvent(e, Constants.getEnrichedSpanConstant(SPAN_TYPE),
        AttributeValueCreator.create(Constants.getEnrichedSpanConstant(ENTRY)));
    return e;
  }

  StructuredTrace createStructuredTrace(String tenantId, Event... events) {
    return StructuredTrace.newBuilder().setCustomerId(tenantId)
        .setTraceId(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()))
        .setStartTimeMillis(System.currentTimeMillis() - 10000)
        .setEndTimeMillis(System.currentTimeMillis())
        .setAttributes(createNewAvroAttributes())
        .setEntityList(new ArrayList<>())
        .setEntityEdgeList(new ArrayList<>())
        .setEventEdgeList(new ArrayList<>())
        .setEntityEventEdgeList(new ArrayList<>())
        .setEventList(Lists.newArrayList(events)).build();
  }

  StructuredTrace createMockStructuredTrace() {
    StructuredTrace trace = mock(StructuredTrace.class);
    when(trace.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());
    when(trace.getMetrics()).thenReturn(Metrics.newBuilder().setMetricMap(new HashMap<>()).build());

    return trace;
  }

  protected void addAttributeToEvent(Event event, String attribute, AttributeValue value) {
    event.getAttributes().getAttributeMap().put(attribute, value);
  }

  protected void addEnrichedAttributeToEvent(Event event, String attribute, AttributeValue value) {
    event.getEnrichedAttributes().getAttributeMap().put(attribute, value);
  }

  Attributes createNewAvroAttributes() {
    return createNewAvroAttributes(Maps.newHashMap());
  }

  protected Attributes createNewAvroAttributes(Map<String, String> attributes) {
    Map<String, AttributeValue> map = new HashMap<>();
    attributes.forEach((k, v) -> map.put(k, createAvroAttribute(v)));
    return Attributes.newBuilder().setAttributeMap(map).build();
  }

  protected Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> createEdsAttributes(
      Map<String, String> attributes) {
    Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> map = new HashMap<>();
    attributes.forEach((k, v) -> map.put(k, createStringAttribute(v)));
    return map;
  }

  org.hypertrace.entity.data.service.v1.AttributeValue createStringAttribute(String value) {
    return org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(
        Value.newBuilder().setString(value)).build();
  }

  protected AttributeValue createAvroAttribute(String value) {
    return AttributeValue.newBuilder().setValue(value).build();
  }

  Event.Builder createOpenSourceSpan(String tenantId, String eventId, String serviceName,
      String spanType) {
    Map<String, String> enrichedAttr = Map.of(Constants.getEnrichedSpanConstant(Api.API_BOUNDARY_TYPE), "ENTRY",
        Constants.getEnrichedSpanConstant(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE), spanType);

    return Event.newBuilder().setCustomerId(tenantId)
        .setEventId(ByteBuffer.wrap(eventId.getBytes()))
        .setEnrichedAttributes(createNewAvroAttributes(enrichedAttr))
        .setServiceName(serviceName)
        .setAttributes(Attributes.newBuilder().build());
  }

  protected StructuredTrace getBigTrace() {
    /*
     * The graph looks like
     *             0 (proxy/entry)
     *             |
     *             1 (i)
     *           /    \
     *          2 (i)  3 (i)
     *          |
     *          4 (proxy/exit)
     *
     * i -> intermediate span
     */
    //0th raw span
    Map<String, AttributeValue> entrySpanProxyMap = new HashMap<>();
    entrySpanProxyMap.put(Constants.getEnrichedSpanConstant(SPAN_TYPE),
        AttributeValue.newBuilder().setValue("ENTRY").build()
    );
    entrySpanProxyMap.put(
        Constants.getEnrichedSpanConstant(Api.API_BOUNDARY_TYPE),
        AttributeValue.newBuilder().setValue("ENTRY").build()
    );

    entrySpanProxyMap.put(
        Constants.getRawSpanConstant(TRACER_TYPE),
        AttributeValue.newBuilder().setValue(PROXY_TRACER).build()
    );
    entrySpanProxyMap.put(Constants.getRawSpanConstant(Docker.DOCKER_CONTAINER_ID),
        AttributeValue.newBuilder().setValue("container1").build()
    );
    Map<String, AttributeValue> enrichedEntrySpanProxyMap = new HashMap<>();
    enrichedEntrySpanProxyMap.put(
        Constants.getEntityConstant(ServiceAttribute.SERVICE_ATTRIBUTE_ID),
        AttributeValue.newBuilder().setValue(COMMON_ENTITY_ID).build()
    );
    enrichedEntrySpanProxyMap.put(
        Constants.getEntityConstant(ServiceAttribute.SERVICE_ATTRIBUTE_NAME),
        AttributeValue.newBuilder().setValue(COMMON_ENTITY_NAME).build()
    );
    Event event0 = Event.newBuilder()
        .setCustomerId(TENANT_ID)
        .setAttributes(org.hypertrace.core.datamodel.Attributes.newBuilder().setAttributeMap(entrySpanProxyMap).build())
        .setEnrichedAttributes(org.hypertrace.core.datamodel.Attributes.newBuilder().setAttributeMap(enrichedEntrySpanProxyMap).build())
        .setEventId(createByteBuffer("event0"))
        .build();
    RawSpan rawSpan0 = RawSpan.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEvent(event0)
        .setTraceId(createByteBuffer("trace"))
        .build();

    //1st raw span CHILD_OF event0
    EventRef eventRef0 = EventRef.newBuilder()
        .setRefType(EventRefType.CHILD_OF)
        .setTraceId(createByteBuffer("trace"))
        .setEventId(createByteBuffer("event0"))
        .build();
    Map<String, AttributeValue> event1EnrichedMap = new HashMap<>();
    event1EnrichedMap.put(Constants.getRawSpanConstant(TRACER_TYPE),
        AttributeValue.newBuilder().setValue(JAVA_TRACER).build());
    event1EnrichedMap.put(Constants.getEnrichedSpanConstant(SPAN_TYPE),
        AttributeValue.newBuilder().setValue("ENTRY").build());
    event1EnrichedMap.put(Constants.getRawSpanConstant(Docker.DOCKER_CONTAINER_ID),
        AttributeValue.newBuilder().setValue("agentContainerId").build());

    Event event1 = Event.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEventId(createByteBuffer("event1"))
        .setEventRefList(Collections.singletonList(eventRef0))
        .setEnrichedAttributes(
            org.hypertrace.core.datamodel.Attributes.newBuilder()
                .setAttributeMap(event1EnrichedMap)
                .build())
        .build();
    RawSpan rawSpan1 = RawSpan.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEvent(event1)
        .setTraceId(createByteBuffer("trace"))
        .build();

    //2nd raw span CHILD_OF event1
    EventRef eventRef1 = EventRef.newBuilder()
        .setRefType(EventRefType.CHILD_OF)
        .setTraceId(createByteBuffer("trace"))
        .setEventId(createByteBuffer("event1"))
        .build();
    Map<String, AttributeValue> event2EnrichedMap = new HashMap<>();
    event2EnrichedMap.put(Constants.getRawSpanConstant(TRACER_TYPE),
        AttributeValue.newBuilder().setValue(JAVA_TRACER).build()
    );
    event2EnrichedMap.put(Constants.getEnrichedSpanConstant(SPAN_TYPE),
        AttributeValue.newBuilder().setValue("EXIT").build());
    event2EnrichedMap.put(Constants.getRawSpanConstant(Docker.DOCKER_CONTAINER_ID),
        AttributeValue.newBuilder().setValue("agentContainerId").build());

    Event event2 = Event.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEventId(createByteBuffer("event2"))
        .setEnrichedAttributes(
            org.hypertrace.core.datamodel.Attributes.newBuilder()
                .setAttributeMap(event2EnrichedMap)
                .build())
        .setEventRefList(Collections.singletonList(eventRef1))
        .build();
    RawSpan rawSpan2 = RawSpan.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEvent(event2)
        .setTraceId(createByteBuffer("trace"))
        .build();

    //3nd raw span CHILD_OF event1
    Event event3 = Event.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEventId(createByteBuffer("event3"))
        .setEventRefList(Collections.singletonList(eventRef1))
        .build();
    RawSpan rawSpan3 = RawSpan.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEvent(event3)
        .setTraceId(createByteBuffer("trace"))
        .build();

    //4th raw span CHILD_OF event2
    EventRef eventRef2 = EventRef.newBuilder()
        .setRefType(EventRefType.CHILD_OF)
        .setTraceId(createByteBuffer("trace"))
        .setEventId(createByteBuffer("event2"))
        .build();

    Map<String, AttributeValue> exitSpanProxyMap = new HashMap<>();
    exitSpanProxyMap.put(
        Constants.getRawSpanConstant(TRACER_TYPE),
        AttributeValue.newBuilder().setValue(PROXY_TRACER).build()
    );
    exitSpanProxyMap.put(Constants.getRawSpanConstant(Docker.DOCKER_CONTAINER_ID),
        AttributeValue.newBuilder().setValue("container1").build());

    Map<String, AttributeValue> exitSpanProxyEnrichedMap = new HashMap<>();
    exitSpanProxyEnrichedMap.put(Constants.getEnrichedSpanConstant(SPAN_TYPE),
        AttributeValue.newBuilder().setValue("EXIT").build());
    exitSpanProxyEnrichedMap.put(Constants.getEnrichedSpanConstant(Api.API_BOUNDARY_TYPE),
        AttributeValue.newBuilder().setValue("EXIT").build()
    );
    exitSpanProxyEnrichedMap.put(
        Constants.getEntityConstant(ServiceAttribute.SERVICE_ATTRIBUTE_ID),
        AttributeValue.newBuilder().setValue(COMMON_ENTITY_ID).build()
    );
    exitSpanProxyEnrichedMap.put(
        Constants.getEntityConstant(ServiceAttribute.SERVICE_ATTRIBUTE_NAME),
        AttributeValue.newBuilder().setValue(COMMON_ENTITY_NAME).build()
    );
    Event event4 = Event.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEventId(createByteBuffer("event4"))
        .setAttributes(
            org.hypertrace.core.datamodel.Attributes.newBuilder()
                .setAttributeMap(exitSpanProxyMap)
                .build())
        .setEnrichedAttributes(
            org.hypertrace.core.datamodel.Attributes.newBuilder()
                .setAttributeMap(exitSpanProxyEnrichedMap)
                .build())
        .setEventRefList(Collections.singletonList(eventRef2))
        .build();
    RawSpan rawSpan4 = RawSpan.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEvent(event4)
        .setTraceId(createByteBuffer("trace"))
        .build();

    return StructuredTraceBuilder.buildStructuredTraceFromRawSpans(
        List.of(rawSpan0, rawSpan1, rawSpan2, rawSpan3, rawSpan4),
        createByteBuffer("trace"),
        TENANT_ID);
  }

  ByteBuffer createByteBuffer(String id) {
    return ByteBuffer.wrap(id.getBytes());
  }

}
