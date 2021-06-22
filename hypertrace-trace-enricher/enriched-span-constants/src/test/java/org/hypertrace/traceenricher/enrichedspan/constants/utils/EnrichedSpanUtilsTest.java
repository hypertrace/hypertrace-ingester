package org.hypertrace.traceenricher.enrichedspan.constants.utils;

import static java.util.Collections.emptyList;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_BODY;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_METHOD;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.entity.constants.v1.ApiAttribute;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.junit.jupiter.api.Test;

public class EnrichedSpanUtilsTest {
  private static final String API_DISCOVERY_STATE_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_DISCOVERY_STATE);

  @Test
  public void testContainsAttributeKey() {
    String attrKey = "somekey";
    {
      Event event = createMockEventWithNoAttributes();
      assertFalse(SpanAttributeUtils.containsAttributeKey(event, attrKey));
    }

    {
      Event event = createMockEventWithAttribute("otherkey", "somevalue");
      assertFalse(SpanAttributeUtils.containsAttributeKey(event, attrKey));
    }

    {
      Event event = createMockEventWithAttribute("somekey", "somevalue");
      assertTrue(SpanAttributeUtils.containsAttributeKey(event, attrKey));
    }

    {
      Event event = createMockEventWithEnrichedAttribute("somekey", "somevalue");
      assertTrue(SpanAttributeUtils.containsAttributeKey(event, attrKey));
    }
  }

  @Test
  public void testGetBooleanAttribute() {
    final String key = "somekey";
    Event event = createMockEventWithNoAttributes();
    assertFalse(SpanAttributeUtils.getBooleanAttribute(event, key));

    event = createMockEventWithAttribute(key, "something");
    assertFalse(SpanAttributeUtils.getBooleanAttribute(event, key));

    event = createMockEventWithAttribute(key, "true");
    assertTrue(SpanAttributeUtils.getBooleanAttribute(event, key));
  }

  @Test
  public void testGetEnrichedAttributesForPrefixAttributeKey() {
    Event event = createMockEventWithEnrichedAttribute("prefix.hello", "world");
    Map<String, AttributeValue> filteredMap =
        SpanAttributeUtils.getAttributesWithPrefixKey(event, "prefix");
    assertEquals("world", filteredMap.get("prefix.hello").getValue());
  }

  @Test
  public void testGetAttributesForPrefixAttributeKey() {
    Event event = createMockEventWithAttribute("prefix.hello", "world");
    Map<String, AttributeValue> filteredMap =
        SpanAttributeUtils.getAttributesWithPrefixKey(event, "prefix");
    assertEquals("world", filteredMap.get("prefix.hello").getValue());
  }

  @Test
  public void testUserAgent() {
    String userAgentValue =
        "Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0";
    List<String> userAgents = EnrichedSpanUtils.USER_AGENT_ATTRIBUTES;
    String userAgentAttribute = userAgents.get(new Random().nextInt(userAgents.size()));
    Event event = createMockEventWithAttribute(userAgentAttribute, userAgentValue);

    assertEquals(userAgentValue, EnrichedSpanUtils.getUserAgent(event));
  }

  @Test
  public void testGetParent() {
    Event e1 = mock(Event.class);
    when(e1.getEventId()).thenReturn(ByteBuffer.wrap("span-1".getBytes()));

    Event e2 = mock(Event.class);
    when(e2.getEventId()).thenReturn(ByteBuffer.wrap("span-2".getBytes()));

    Event e3 = mock(Event.class);
    when(e3.getEventId()).thenReturn(ByteBuffer.wrap("span-3".getBytes()));

    Event e4 = mock(Event.class);
    when(e4.getEventId()).thenReturn(ByteBuffer.wrap("span-4".getBytes()));

    Map<ByteBuffer, Event> idToEvent =
        Map.of(
            ByteBuffer.wrap("span-1".getBytes()), e1,
            ByteBuffer.wrap("span-2".getBytes()), e2,
            ByteBuffer.wrap("span-3".getBytes()), e3,
            ByteBuffer.wrap("span-4".getBytes()), e4);

    Map<ByteBuffer, ByteBuffer> childToParentEventIds =
        Map.of(
            ByteBuffer.wrap("span-3".getBytes()), ByteBuffer.wrap("span-2".getBytes()),
            ByteBuffer.wrap("span-2".getBytes()), ByteBuffer.wrap("span-1".getBytes()),
            ByteBuffer.wrap("span-4".getBytes()), ByteBuffer.wrap("unknown".getBytes()));

    Event parent = SpanAttributeUtils.getParentSpan(e3, childToParentEventIds, idToEvent);
    assertEquals(e2, parent);

    parent = SpanAttributeUtils.getParentSpan(e2, childToParentEventIds, idToEvent);
    assertEquals(e1, parent);

    parent = SpanAttributeUtils.getParentSpan(e1, childToParentEventIds, idToEvent);
    assertEquals(null, parent);

    parent = SpanAttributeUtils.getParentSpan(e4, childToParentEventIds, idToEvent);
    assertEquals(null, parent);
  }

  @Test
  public void testGetStringAttributeIgnoreKeyCase() {
    assertTrue(SpanAttributeUtils.getStringAttributeIgnoreKeyCase(null, "k2").isEmpty());

    Event e1 = mock(Event.class);
    assertTrue(SpanAttributeUtils.getStringAttributeIgnoreKeyCase(e1, "k2").isEmpty());

    when(e1.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(Map.of("K2", AttributeValue.newBuilder().setValue("v2").build()))
                .build());
    // Should be case insensitive
    assertEquals("v2", SpanAttributeUtils.getStringAttributeIgnoreKeyCase(e1, "k2").get());
    assertEquals("v2", SpanAttributeUtils.getStringAttributeIgnoreKeyCase(e1, "K2").get());
    assertTrue(SpanAttributeUtils.getStringAttributeIgnoreKeyCase(e1, "k3").isEmpty());

    when(e1.getEnrichedAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(Map.of("k3", AttributeValue.newBuilder().setValue("v3").build()))
                .build());
    // Should be case insensitive
    assertEquals("v3", SpanAttributeUtils.getStringAttributeIgnoreKeyCase(e1, "k3").get());
    assertEquals("v3", SpanAttributeUtils.getStringAttributeIgnoreKeyCase(e1, "K3").get());

    assertTrue(SpanAttributeUtils.getStringAttributeIgnoreKeyCase(e1, "k4").isEmpty());
  }

  @Test
  public void should_getApiDiscoveryState_enrichedAttribute() {
    Event e = createMockEventWithEnrichedAttribute(API_DISCOVERY_STATE_ATTR, "DISCOVERED");
    assertEquals("DISCOVERED", EnrichedSpanUtils.getApiDiscoveryState(e));
  }

  @Test
  public void should_getHttpMethod() {
    Event e = createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_METHOD), "GET");
    Optional<String> method = EnrichedSpanUtils.getHttpMethod(e);
    assertFalse(method.isEmpty());
    assertEquals("GET", method.get());
  }

  @Test
  public void should_getNullMethod_noHttpFields() {
    Event e = mock(Event.class);
    when(e.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());

    Optional<String> method = EnrichedSpanUtils.getHttpMethod(e);
    assertTrue(method.isEmpty());
  }

  @Test
  public void should_getFullUrl() {
    String testurl = "http://hipstershop.com?order=1";
    Event e = createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_URL), testurl);
    Optional<String> url = EnrichedSpanUtils.getFullHttpUrl(e);
    assertFalse(url.isEmpty());
    assertEquals(testurl, url.get());
  }

  @Test
  public void should_getNullUrl_noHttpFields() {
    Event e = mock(Event.class);
    when(e.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());

    Optional<String> url = EnrichedSpanUtils.getFullHttpUrl(e);
    assertTrue(url.isEmpty());
  }

  @Test
  public void getRequestSize_httpProtocol() {
    Event e = createMockEventWithEnrichedAttribute("PROTOCOL", "HTTP");
    when(e.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());
    addAttribute(e, RawSpanConstants.getValue(HTTP_REQUEST_SIZE), "64");

    Optional<Integer> requestSize = EnrichedSpanUtils.getRequestSize(e);
    assertFalse(requestSize.isEmpty());
    assertEquals(64, requestSize.get().intValue());
  }

  @Test
  public void getRequestSize_grpcProtocol() {
    Event e = createMockEventWithEnrichedAttribute("PROTOCOL", "GRPC");
    when(e.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());
    addAttribute(e, RawSpanConstants.getValue(GRPC_REQUEST_BODY), "some grpc response body");

    Optional<Integer> requestSize = EnrichedSpanUtils.getRequestSize(e);
    assertFalse(requestSize.isEmpty());
    assertEquals(23, requestSize.get().intValue());
  }

  @Test
  public void getRequestSize_emptyProtocol() {
    Event e = mock(Event.class);

    Optional<Integer> requestSize = EnrichedSpanUtils.getRequestSize(e);
    assertTrue(requestSize.isEmpty());
  }

  @Test
  public void getRequestSize_httpProtocol_noSize() {
    Event e = createMockEventWithEnrichedAttribute("PROTOCOL", "HTTP");
    when(e.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());

    Optional<Integer> requestSize = EnrichedSpanUtils.getRequestSize(e);
    assertTrue(requestSize.isEmpty());
  }

  @Test
  public void getResponseSize_httpProtocol() {
    Event e = createMockEventWithEnrichedAttribute("PROTOCOL", "HTTP");
    when(e.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());
    addAttribute(e, RawSpanConstants.getValue(HTTP_RESPONSE_SIZE), "64");

    Optional<Integer> responseSize = EnrichedSpanUtils.getResponseSize(e);
    assertFalse(responseSize.isEmpty());
    assertEquals(64, responseSize.get().intValue());
  }

  @Test
  public void getResponseSize_grpcProtocol() {
    Event e = createMockEventWithEnrichedAttribute("PROTOCOL", "GRPC");
    when(e.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());
    addAttribute(e, RawSpanConstants.getValue(GRPC_RESPONSE_BODY), "some grpc request body");

    Optional<Integer> responseSize = EnrichedSpanUtils.getResponseSize(e);
    assertFalse(responseSize.isEmpty());
    assertEquals(22, responseSize.get().intValue());
  }

  @Test
  public void getResponseSize_httpProtocol_noSize() {
    Event e = createMockEventWithEnrichedAttribute("PROTOCOL", "HTTP");
    when(e.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());

    Optional<Integer> requestSize = EnrichedSpanUtils.getResponseSize(e);
    assertTrue(requestSize.isEmpty());
  }

  @Test
  public void getResponseSize_emptyProtocol() {
    Event e = mock(Event.class);

    Optional<Integer> requestSize = EnrichedSpanUtils.getResponseSize(e);
    assertTrue(requestSize.isEmpty());
  }

  @Test
  public void testGetSpaceIds_empty() {
    Event e = mock(Event.class);

    List<String> spaceIds = EnrichedSpanUtils.getSpaceIds(e);
    assertEquals(emptyList(), spaceIds);
  }

  @Test
  public void testGetBackendOperation_empty() {
    Event e = mock(Event.class);

    String backend_operation = EnrichedSpanUtils.getBackendOperation(e);
    assertNull(backend_operation);
  }

  @Test
  public void testGetBackendOperation_withData() {
    Event e = createMockEventWithEnrichedAttribute("BACKEND_OPERATION", "select");

    assertEquals("select", EnrichedSpanUtils.getBackendOperation(e));
  }

  @Test
  public void testGetBackendDestination_empty() {
    Event e = mock(Event.class);

    String backend_destination = EnrichedSpanUtils.getBackendDestination(e);
    assertNull(backend_destination);
  }

  @Test
  public void testGetBackendDestination_withData() {
    Event e = createMockEventWithEnrichedAttribute("BACKEND_DESTINATION", "tableName");

    assertEquals("tableName", EnrichedSpanUtils.getBackendDestination(e));
  }

  @Test
  public void testGetSpaceIds_withData() {
    List<String> spaceIds = List.of("space1", "space2");
    Event e = mock(Event.class);
    when(e.getEnrichedAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(Map.of("SPACE_IDS", AttributeValueCreator.create(spaceIds)))
                .build());

    assertEquals(spaceIds, EnrichedSpanUtils.getSpaceIds(e));
  }

  private void addAttribute(Event event, String key, String val) {
    event
        .getAttributes()
        .getAttributeMap()
        .put(key, AttributeValue.newBuilder().setValue(val).build());
  }

  private Event createMockEventWithNoAttributes() {
    Event e = mock(Event.class);
    when(e.getAttributes()).thenReturn(null);
    when(e.getEnrichedAttributes()).thenReturn(null);
    return e;
  }

  private Event createMockEventWithAttribute(String key, String value) {
    Event e = mock(Event.class);
    when(e.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(Map.of(key, AttributeValue.newBuilder().setValue(value).build()))
                .build());
    when(e.getEnrichedAttributes()).thenReturn(null);
    return e;
  }

  private Event createMockEventWithEnrichedAttribute(String key, String value) {
    Event e = mock(Event.class);
    when(e.getAttributes()).thenReturn(null);
    when(e.getEnrichedAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(Map.of(key, AttributeValue.newBuilder().setValue(value).build()))
                .build());
    return e;
  }
}
