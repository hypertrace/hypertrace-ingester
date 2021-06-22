package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SYSTEM;
import static org.hypertrace.traceenricher.enrichment.enrichers.SpanTypeAttributeEnricher.CLIENT_KEY;
import static org.hypertrace.traceenricher.enrichment.enrichers.SpanTypeAttributeEnricher.CLIENT_VALUE;
import static org.hypertrace.traceenricher.enrichment.enrichers.SpanTypeAttributeEnricher.SERVER_VALUE;
import static org.hypertrace.traceenricher.enrichment.enrichers.SpanTypeAttributeEnricher.SPAN_KIND_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.eventfields.rpc.Rpc;
import org.hypertrace.core.semantic.convention.constants.messaging.OtelMessagingSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.v1.Envoy;
import org.hypertrace.core.span.constants.v1.Grpc;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.core.span.constants.v1.SpanAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SpanTypeAttributeEnricherTest extends AbstractAttributeEnricherTest {

  @Test
  public void noAttributes() {
    SpanTypeAttributeEnricher enricher = new SpanTypeAttributeEnricher();
    Event e = mock(Event.class);
    when(e.getAttributes()).thenReturn(null);
    enricher.enrichEvent(null, e);

    when(e.getAttributes()).thenReturn(new Attributes());
    enricher.enrichEvent(null, e);
  }

  @Test
  public void spanKindExists() {
    SpanTypeAttributeEnricher enricher = new SpanTypeAttributeEnricher();
    Event e = createMockEvent();
    addAttribute(e, SPAN_KIND_KEY, SERVER_VALUE);
    enricher.enrichEvent(null, e);
    Map<String, AttributeValue> enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(ENTRY));

    e = createMockEvent();
    addAttribute(
        e,
        OTelSpanSemanticConventions.SPAN_KIND.getValue(),
        OTelSpanSemanticConventions.SPAN_KIND_SERVER_VALUE.getValue());
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        Constants.getEnrichedSpanConstant(ENTRY),
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue());

    e = createMockEvent();
    addAttribute(
        e,
        OTelSpanSemanticConventions.SPAN_KIND.getValue(),
        OtelMessagingSemanticConventions.CONSUMER.getValue());
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        Constants.getEnrichedSpanConstant(ENTRY),
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue());

    e = createMockEvent();
    addAttribute(e, SPAN_KIND_KEY, OtelMessagingSemanticConventions.CONSUMER.getValue());
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        Constants.getEnrichedSpanConstant(ENTRY),
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue());

    e = createMockEvent();
    addAttribute(e, SPAN_KIND_KEY, CLIENT_VALUE);
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(EXIT));

    e = createMockEvent();
    addAttribute(
        e,
        OTelSpanSemanticConventions.SPAN_KIND.getValue(),
        OTelSpanSemanticConventions.SPAN_KIND_CLIENT_VALUE.getValue());
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(EXIT));

    e = createMockEvent();
    addAttribute(
        e,
        OTelSpanSemanticConventions.SPAN_KIND.getValue(),
        OtelMessagingSemanticConventions.PRODUCER.getValue());
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(EXIT));

    e = createMockEvent();
    addAttribute(e, SPAN_KIND_KEY, OtelMessagingSemanticConventions.PRODUCER.getValue());
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(EXIT));

    e = createMockEvent();
    addAttribute(e, SPAN_KIND_KEY, "unknown");
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(UNKNOWN));
  }

  @Test
  public void clientExists() {
    SpanTypeAttributeEnricher enricher = new SpanTypeAttributeEnricher();
    Event e = createMockEvent();
    addAttribute(e, CLIENT_KEY, "false");
    enricher.enrichEvent(null, e);
    Map<String, AttributeValue> enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(ENTRY));

    e = createMockEvent();
    addAttribute(e, CLIENT_KEY, "true");
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(EXIT));

    e = createMockEvent();
    addAttribute(e, CLIENT_KEY, "unknown");
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(UNKNOWN));
  }

  @Test
  public void spanNameConvention() {
    SpanTypeAttributeEnricher enricher = new SpanTypeAttributeEnricher();
    Event e = createMockEvent();
    when(e.getEventName()).thenReturn("Sent./a/");
    enricher.enrichEvent(null, e);
    Map<String, AttributeValue> enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(EXIT));

    e = createMockEvent();
    when(e.getEventName()).thenReturn("Recv./b/");
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(ENTRY));
  }

  @Test
  public void envoySpanCategorization() {
    SpanTypeAttributeEnricher enricher = new SpanTypeAttributeEnricher();
    // No operation name
    Event e = createMockEnvoyEvent(null);
    enricher.enrichEvent(null, e);
    Map<String, AttributeValue> enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(UNKNOWN));

    // Ingress operation should be Entry span.
    e = createMockEnvoyEvent(Constants.getRawSpanConstant(Envoy.ENVOY_INGRESS_SPAN));
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(ENTRY));

    // Egress operation should be Exit span
    e = createMockEnvoyEvent(Constants.getRawSpanConstant(Envoy.ENVOY_EGRESS_SPAN));
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(EXIT));

    // Random value is Unknown span
    e = createMockEnvoyEvent("Random");
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(UNKNOWN));
  }

  @Test
  public void multipleAttributesExistsPrecedence() {
    SpanTypeAttributeEnricher enricher = new SpanTypeAttributeEnricher();

    // span.kind takes precedence
    Event e = createMockEvent();
    when(e.getEventName()).thenReturn("Sent./api/10");
    addAttribute(e, SPAN_KIND_KEY, SERVER_VALUE);
    enricher.enrichEvent(null, e);
    Map<String, AttributeValue> enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(ENTRY));

    // Then Client
    e = createMockEvent();
    addAttribute(e, CLIENT_KEY, "false");
    when(e.getEventName()).thenReturn("Sent./api/10");
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(ENTRY));

    // Finally span name convention
    e = createMockEvent();
    when(e.getEventName()).thenReturn("Sent./api/10");
    enricher.enrichEvent(null, e);
    enrichedAttributes = e.getEnrichedAttributes().getAttributeMap();
    Assertions.assertEquals(
        enrichedAttributes.get(Constants.getEnrichedSpanConstant(SPAN_TYPE)).getValue(),
        Constants.getEnrichedSpanConstant(EXIT));
  }

  private Event createEvent(
      Map<String, AttributeValue> attributeMap, Map<String, AttributeValue> enrichedAttributeMap) {
    return Event.newBuilder()
        .setCustomerId(TENANT_ID)
        .setEventId(ByteBuffer.wrap("event1".getBytes()))
        .setAttributes(Attributes.newBuilder().setAttributeMap(attributeMap).build())
        .setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(enrichedAttributeMap).build())
        .build();
  }

  @Test
  public void test_getProtocolName_OTgrpc_shouldReturnGrpc() {
    Map<String, AttributeValue> map = new HashMap<>();
    map.put(
        Constants.getRawSpanConstant(OTSpanTag.OT_SPAN_TAG_COMPONENT),
        AttributeValue.newBuilder()
            .setValue(Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_GRPC))
            .build());

    map.put(
        Constants.getRawSpanConstant(Http.HTTP_REQUEST_METHOD),
        AttributeValue.newBuilder().setValue("GET").build());

    Event e = createEvent(map, new HashMap<>());
    Assertions.assertEquals(Protocol.PROTOCOL_GRPC, SpanTypeAttributeEnricher.getProtocolName(e));

    SpanTypeAttributeEnricher enricher = new SpanTypeAttributeEnricher();
    enricher.enrichEvent(null, e);
    Assertions.assertEquals(Protocol.PROTOCOL_GRPC, EnrichedSpanUtils.getProtocol(e));
  }

  @Test
  public void test_getProtocolName_Otelgrpc_shouldReturnGrpc() {
    Map<String, AttributeValue> map = new HashMap<>();
    map.put(
        Constants.getRawSpanConstant(Http.HTTP_REQUEST_METHOD),
        AttributeValue.newBuilder().setValue("GET").build());

    Event e = createEvent(map, new HashMap<>());
    e.setRpc(Rpc.newBuilder().setSystem("grpc").build());
    addAttribute(e, OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), "grpc");
    Assertions.assertEquals(Protocol.PROTOCOL_GRPC, SpanTypeAttributeEnricher.getProtocolName(e));
  }

  @Test
  public void test_getProtocolName_grpcAndHttp_shouldReturnGrpc() {
    Map<String, AttributeValue> map = new HashMap<>();
    map.put(
        Constants.getRawSpanConstant(Grpc.GRPC_METHOD),
        AttributeValue.newBuilder().setValue("grpc method").build());

    map.put(
        Constants.getRawSpanConstant(Http.HTTP_REQUEST_METHOD),
        AttributeValue.newBuilder().setValue("GET").build());
    Event e = createEvent(map, new HashMap<>());

    Assertions.assertEquals(Protocol.PROTOCOL_GRPC, SpanTypeAttributeEnricher.getProtocolName(e));
  }

  @Test
  public void test_getProtocolName_grpcAttributes_shouldReturnGrpc() {
    Map<String, AttributeValue> map = new HashMap<>();

    map.put(
        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), AttributeValue.newBuilder().setValue("grpc").build());
    Event e = createEvent(map, new HashMap<>());

    Assertions.assertEquals(Protocol.PROTOCOL_GRPC, SpanTypeAttributeEnricher.getProtocolName(e));
  }

  @Test
  public void test_getProtocolName_HttpFromUrl_shouldReturnHttp() {
    Map<String, AttributeValue> map = new HashMap<>();

    map.put(
        Constants.getRawSpanConstant(Http.HTTP_REQUEST_URL),
        AttributeValue.newBuilder().setValue("http://hypertrace.org").build());
    Event e = createEvent(map, new HashMap<>());

    Assertions.assertEquals(Protocol.PROTOCOL_HTTP, SpanTypeAttributeEnricher.getProtocolName(e));
  }

  @Test
  public void test_getProtocolName_HttpFromFullUrl_shouldReturnHttp() {
    Map<String, AttributeValue> map = new HashMap<>();

    map.put(
        Constants.getRawSpanConstant(Http.HTTP_METHOD),
        AttributeValue.newBuilder().setValue("GET").build());
    Event e = createEvent(map, new HashMap<>());
    e.setHttp(
        org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder()
            .setRequest(Request.newBuilder().setUrl("http://hypertrace.org").build())
            .build());

    Assertions.assertEquals(Protocol.PROTOCOL_HTTP, SpanTypeAttributeEnricher.getProtocolName(e));
  }

  @Test
  public void test_getProtocolName_httpAttributes_shouldReturnHttp() {
    Map<String, AttributeValue> map = new HashMap<>();

    map.put(
        Constants.getRawSpanConstant(Http.HTTP_REQUEST_URL),
        AttributeValue.newBuilder().setValue("http://hypertrace.org").build());
    map.put(
        Constants.getRawSpanConstant(Http.HTTP_REQUEST_METHOD),
        AttributeValue.newBuilder().setValue("GET").build());
    Event e = createEvent(map, new HashMap<>());

    Assertions.assertEquals(Protocol.PROTOCOL_HTTP, SpanTypeAttributeEnricher.getProtocolName(e));
  }

  @Test
  public void test_getProtocolName_noHttpAndGrpc_shouldReturnUnknown() {
    Map<String, AttributeValue> map = new HashMap<>();

    map.put(
        Constants.getRawSpanConstant(SpanAttribute.SPAN_ATTRIBUTE_SPAN_KIND),
        AttributeValue.newBuilder().setValue("ENTRY").build());
    Event e = createEvent(map, new HashMap<>());

    Assertions.assertEquals(
        Protocol.PROTOCOL_UNSPECIFIED, SpanTypeAttributeEnricher.getProtocolName(e));
  }

  @Test
  public void test_getProtocolNameWithGrpcEventName_noOTandGrpcTags_shouldReturnUnspecified() {
    Event e = createEvent(new HashMap<>(), new HashMap<>());
    e.setEventName("Sent./products/browse");
    Assertions.assertEquals(
        Protocol.PROTOCOL_UNSPECIFIED, SpanTypeAttributeEnricher.getProtocolName(e));

    e.setEventName("Recv./products/browse");
    Assertions.assertEquals(
        Protocol.PROTOCOL_UNSPECIFIED, SpanTypeAttributeEnricher.getProtocolName(e));
  }

  @Test
  public void test_getProtocolNameWithNonGrpcEventName_noOTandGrpcTags_shouldReturnUnknown() {
    Event e = createEvent(new HashMap<>(), new HashMap<>());
    e.setEventName("ingress");
    Assertions.assertEquals(
        Protocol.PROTOCOL_UNSPECIFIED, SpanTypeAttributeEnricher.getProtocolName(e));
  }

  private void addAttribute(Event event, String key, String val) {
    event
        .getAttributes()
        .getAttributeMap()
        .put(key, AttributeValue.newBuilder().setValue(val).build());
  }
}
