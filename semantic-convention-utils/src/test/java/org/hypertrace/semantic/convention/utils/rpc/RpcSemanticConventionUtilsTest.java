package org.hypertrace.semantic.convention.utils.rpc;

import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_BODY;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SYSTEM;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.semantic.convention.constants.rpc.OTelRpcSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Grpc;
import org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil;
import org.junit.jupiter.api.Test;

/** Unit test for {@link RpcSemanticConventionUtils} */
class RpcSemanticConventionUtilsTest {

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

  private void addAttribute(Event event, String key, String val) {
    event
        .getAttributes()
        .getAttributeMap()
        .put(key, AttributeValue.newBuilder().setValue(val).build());
  }

  @Test
  public void testIsRpcTypeGrpcForOTelFormat() {
    Event e = mock(Event.class);
    // otel format
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelRpcSemanticConventions.RPC_SYSTEM.getValue(),
                SemanticConventionTestUtil.buildAttributeValue(
                    OTelRpcSemanticConventions.RPC_SYSTEM_VALUE_GRPC.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = RpcSemanticConventionUtils.isRpcTypeGrpcForOTelFormat(e);
    assertTrue(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelRpcSemanticConventions.RPC_SYSTEM.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("other")));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcSemanticConventionUtils.isRpcTypeGrpcForOTelFormat(e);
    assertFalse(v);
  }

  @Test
  void getGrpcURI() {
    Event e = mock(Event.class);

    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Grpc.GRPC_HOST_PORT),
                SemanticConventionTestUtil.buildAttributeValue("webhost:9011")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = RpcSemanticConventionUtils.getGrpcURI(e);
    assertEquals("webhost:9011", v.get());

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of("span.kind", SemanticConventionTestUtil.buildAttributeValue("client")));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcSemanticConventionUtils.getGrpcURI(e);
    assertTrue(v.isEmpty());
  }

  @Test
  public void testGetRpcDestination() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelRpcSemanticConventions.RPC_SYSTEM_SERVICE.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("testService")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = RpcSemanticConventionUtils.getRpcService(e);
    assertEquals("testService", v.get());
  }

  @Test
  public void testIfRpcDestinationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(RpcSemanticConventionUtils.getRpcService(e).isEmpty());
  }

  @Test
  public void testGetGrpcStatusCode() {
    Event event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RawSpanConstants.getValue(Grpc.GRPC_STATUS_CODE),
                        AttributeValue.newBuilder().setValue("5").build(),
                        RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_CODE),
                        AttributeValue.newBuilder().setValue("12").build(),
                        RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE),
                        AttributeValue.newBuilder().setValue("14").build()))
                .build());
    assertEquals(12, RpcSemanticConventionUtils.getGrpcStatusCode(event));
  }

  @Test
  public void testGetGrpcXForwardedFor() {
    Event event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(),
                        AttributeValue.newBuilder().setValue("grpc").build(),
                        RPC_REQUEST_METADATA_X_FORWARDED_FOR.getValue(),
                        AttributeValue.newBuilder().setValue("198.12.34.1").build()))
                .build());
    assertEquals("198.12.34.1", RpcSemanticConventionUtils.getGrpcXForwardedFor(event).get());
  }

  @Test
  public void testGetGrpcStatusMsg() {
    Event event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE), "msg 1");
    assertEquals("msg 1", RpcSemanticConventionUtils.getGrpcStatusMsg(event));
  }

  @Test
  public void testGetGrpcErrorMsg() {
    Event event =
        createMockEventWithAttribute(RawSpanConstants.getValue(GRPC_ERROR_MESSAGE), "e_msg 1");
    assertEquals("e_msg 1", RpcSemanticConventionUtils.getGrpcErrorMsg(event));
  }

  @Test
  public void testGetGrpcUserAgent() {
    Event event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RPC_REQUEST_METADATA_USER_AGENT.getValue(),
                        AttributeValue.newBuilder().setValue("abc").build(),
                        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(),
                        AttributeValue.newBuilder().setValue("grpc").build()))
                .build());
    assertEquals(Optional.of("abc"), RpcSemanticConventionUtils.getGrpcUserAgent(event));

    event = mock(Event.class);
    assertTrue(RpcSemanticConventionUtils.getGrpcUserAgent(event).isEmpty());

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RPC_REQUEST_METADATA_USER_AGENT.getValue(),
                        AttributeValue.newBuilder().setValue("abc").build(),
                        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(),
                        AttributeValue.newBuilder().setValue("").build()))
                .build());
    assertTrue(RpcSemanticConventionUtils.getGrpcUserAgent(event).isEmpty());
  }

  @Test
  public void testGetGrpcAuthrity() {
    Event event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RPC_REQUEST_METADATA_AUTHORITY.getValue(),
                        AttributeValue.newBuilder().setValue("abc").build(),
                        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(),
                        AttributeValue.newBuilder().setValue("grpc").build()))
                .build());
    assertEquals(Optional.of("abc"), RpcSemanticConventionUtils.getGrpcAuthority(event));

    event = mock(Event.class);
    assertTrue(RpcSemanticConventionUtils.getGrpcAuthority(event).isEmpty());

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RPC_REQUEST_METADATA_AUTHORITY.getValue(),
                        AttributeValue.newBuilder().setValue("abc").build(),
                        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(),
                        AttributeValue.newBuilder().setValue("").build()))
                .build());
    assertTrue(RpcSemanticConventionUtils.getGrpcAuthority(event).isEmpty());
  }

  @Test
  public void testGetGrpcRequestSize() {
    Event event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(GRPC_REQUEST_BODY), "some grpc request body");
    assertEquals(Optional.of(22), RpcSemanticConventionUtils.getGrpcRequestSize(event));

    event = mock(Event.class);
    assertTrue(RpcSemanticConventionUtils.getGrpcRequestSize(event).isEmpty());
  }

  @Test
  public void testGetGrpcResponseSize() {
    Event event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(GRPC_RESPONSE_BODY), "some grpc response body");
    assertEquals(Optional.of(23), RpcSemanticConventionUtils.getGrpcResponseSize(event));

    event = mock(Event.class);
    assertTrue(RpcSemanticConventionUtils.getGrpcResponseSize(event).isEmpty());
  }
}
