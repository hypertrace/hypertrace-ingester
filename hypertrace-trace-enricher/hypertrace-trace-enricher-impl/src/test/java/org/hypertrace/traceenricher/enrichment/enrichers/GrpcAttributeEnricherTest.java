package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class GrpcAttributeEnricherTest extends AbstractAttributeEnricherTest {

  @Mock private StructuredTrace mockTrace;

  private final GrpcAttributeEnricher enricher = new GrpcAttributeEnricher();

  /*
   * Covers cases of GrpcAttributeEnricher, the details cases of getGrpcRequestEndpoint
   * are covered at RpcSemanticConventionUtilsTest
   * */
  @Test
  public void test_withAValidUrl_shouldEnrichHttpPathAndParams() {
    // case 1: using event name prefixed with Recv
    Event e = createMockEvent();
    when(e.getEventName()).thenReturn("Recv.TestService.GetEventEchos");
    addAttribute(
        e, EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_PROTOCOL), "GRPC");
    addAttribute(
        e, EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE), "ENTRY");

    enricher.enrichEvent(mockTrace, e);

    String grpcRequestUrl =
        SpanAttributeUtils.getStringAttribute(
            e, EnrichedSpanConstants.GRPC_REQUEST_URL_FORMAT_DOTTED);
    assertEquals("Recv.TestService.GetEventEchos", grpcRequestUrl);

    String grpcRequestEndPoint =
        SpanAttributeUtils.getStringAttribute(
            e, EnrichedSpanConstants.GRPC_REQUEST_ENDPOINT_FORMAT_DOTTED);
    assertEquals("TestService.GetEventEchos", grpcRequestEndPoint);

    // case 2: using grpc.path
    e = createMockEvent();
    when(e.getEventName()).thenReturn("TestService.GetEventEchos");
    addAttribute(
        e, EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_PROTOCOL), "GRPC");
    addAttribute(
        e, EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE), "ENTRY");
    addAttribute(e, "grpc.path", "/TestGrpcService/GetGrpcPathEchos");

    enricher.enrichEvent(mockTrace, e);

    grpcRequestUrl =
        SpanAttributeUtils.getStringAttribute(
            e, EnrichedSpanConstants.GRPC_REQUEST_URL_FORMAT_DOTTED);
    assertEquals("Recv.TestGrpcService.GetGrpcPathEchos", grpcRequestUrl);

    grpcRequestEndPoint =
        SpanAttributeUtils.getStringAttribute(
            e, EnrichedSpanConstants.GRPC_REQUEST_ENDPOINT_FORMAT_DOTTED);
    assertEquals("TestGrpcService.GetGrpcPathEchos", grpcRequestEndPoint);

    // case 3: no grpc protocol
    e = createMockEvent();
    when(e.getEventName()).thenReturn("TestService.GetEventEchos");
    addAttribute(
        e, EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE), "ENTRY");
    addAttribute(e, "grpc.path", "/TestGrpcService/GetGrpcPathEchos");

    enricher.enrichEvent(mockTrace, e);

    grpcRequestUrl =
        SpanAttributeUtils.getStringAttribute(
            e, EnrichedSpanConstants.GRPC_REQUEST_URL_FORMAT_DOTTED);
    assertNull(grpcRequestUrl);

    grpcRequestEndPoint =
        SpanAttributeUtils.getStringAttribute(
            e, EnrichedSpanConstants.GRPC_REQUEST_ENDPOINT_FORMAT_DOTTED);
    assertNull(grpcRequestEndPoint);

    // case 4: client call - EXIT
    e = createMockEvent();
    when(e.getEventName()).thenReturn("TestService.GetEventEchos");
    addAttribute(
        e, EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_PROTOCOL), "GRPC");
    addAttribute(
        e, EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE), "EXIT");
    addAttribute(e, "grpc.path", "/TestGrpcService/GetGrpcPathEchos");

    enricher.enrichEvent(mockTrace, e);

    grpcRequestUrl =
        SpanAttributeUtils.getStringAttribute(
            e, EnrichedSpanConstants.GRPC_REQUEST_URL_FORMAT_DOTTED);
    assertEquals("Sent.TestGrpcService.GetGrpcPathEchos", grpcRequestUrl);

    grpcRequestEndPoint =
        SpanAttributeUtils.getStringAttribute(
            e, EnrichedSpanConstants.GRPC_REQUEST_ENDPOINT_FORMAT_DOTTED);
    assertEquals("TestGrpcService.GetGrpcPathEchos", grpcRequestEndPoint);

    // case 5: no entry or exsit span (internal span) - not prefix sent / recv
    e = createMockEvent();
    when(e.getEventName()).thenReturn("TestService.GetEventEchos");
    addAttribute(
        e, EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_PROTOCOL), "GRPC");
    addAttribute(e, "grpc.path", "/TestGrpcService/GetGrpcPathEchos");

    enricher.enrichEvent(mockTrace, e);

    grpcRequestUrl =
        SpanAttributeUtils.getStringAttribute(
            e, EnrichedSpanConstants.GRPC_REQUEST_URL_FORMAT_DOTTED);
    assertEquals("TestGrpcService.GetGrpcPathEchos", grpcRequestUrl);

    grpcRequestEndPoint =
        SpanAttributeUtils.getStringAttribute(
            e, EnrichedSpanConstants.GRPC_REQUEST_ENDPOINT_FORMAT_DOTTED);
    assertEquals("TestGrpcService.GetGrpcPathEchos", grpcRequestEndPoint);
  }

  private void addAttribute(Event event, String key, String val) {
    event
        .getAttributes()
        .getAttributeMap()
        .put(key, AttributeValue.newBuilder().setValue(val).build());
  }
}
