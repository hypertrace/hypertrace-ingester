package org.hypertrace.semantic.convention.utils.rpc;

import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_BODY_TRUNCATED;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_BODY_TRUNCATED;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_METHOD;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SYSTEM;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_BODY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_BODY_TRUNCATED;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_AUTHORITY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_CONTENT_LENGTH;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_HOST;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_PATH;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_USER_AGENT;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_X_FORWARDED_FOR;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_BODY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_BODY_TRUNCATED;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_METADATA_CONTENT_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.semantic.convention.constants.error.OTelErrorSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.rpc.OTelRpcSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
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
  public void testGetGrpcRequestMetadataHost() {
    Event event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(),
                        AttributeValue.newBuilder().setValue("grpc").build(),
                        RPC_REQUEST_METADATA_HOST.getValue(),
                        AttributeValue.newBuilder().setValue("webhost:9011").build()))
                .build());
    assertEquals(
        "webhost:9011", RpcSemanticConventionUtils.getGrpcRequestMetadataHost(event).get());
  }

  @Test
  public void testGetGrpcStatusMsg() {
    Event event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE), "msg 1");
    assertEquals("msg 1", RpcSemanticConventionUtils.getGrpcStatusMsg(event));
  }

  @Test
  public void testRpcMethod() {
    Event event = createMockEventWithAttribute(OTEL_SPAN_TAG_RPC_METHOD.getValue(), "GetId");
    assertEquals("GetId", RpcSemanticConventionUtils.getRpcMethod(event).get());
  }

  @Test
  public void testRpcService() {
    Event event =
        createMockEventWithAttribute(
            OTelRpcSemanticConventions.RPC_SYSTEM_SERVICE.getValue(), "package.service");
    assertEquals("package.service", RpcSemanticConventionUtils.getRpcService(event).get());
  }

  @Test
  public void testGetGrpcRequestMetadataPath() {
    Event event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        "rpc.system",
                        AttributeValue.newBuilder().setValue("grpc").build(),
                        RPC_REQUEST_METADATA_PATH.getValue(),
                        AttributeValue.newBuilder().setValue("/package.service/GetId").build()))
                .build());

    assertEquals(
        "/package.service/GetId",
        RpcSemanticConventionUtils.getGrpcRequestMetadataPath(event).get());
  }

  @Test
  public void testGetGrpcErrorMsg() {
    Event event =
        createMockEventWithAttribute(RawSpanConstants.getValue(GRPC_ERROR_MESSAGE), "e_msg 1");
    assertEquals("e_msg 1", RpcSemanticConventionUtils.getGrpcErrorMsg(event));

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        "rpc.system",
                        AttributeValue.newBuilder().setValue("grpc").build(),
                        RPC_ERROR_MESSAGE.getValue(),
                        AttributeValue.newBuilder().setValue("e_msg 2").build()))
                .build());
    assertEquals("e_msg 2", RpcSemanticConventionUtils.getGrpcErrorMsg(event));
  }

  @Test
  public void testGetGrpcErrorMsgPriority() {

    Event event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        OTelRpcSemanticConventions.RPC_SYSTEM.getValue(),
                        AttributeValue.newBuilder().setValue("grpc").build(),
                        OTelErrorSemanticConventions.EXCEPTION_MESSAGE.getValue(),
                        AttributeValue.newBuilder().setValue("exc_msg").build(),
                        RawSpanConstants.getValue(GRPC_ERROR_MESSAGE),
                        AttributeValue.newBuilder().setValue("e_msg 1").build(),
                        RPC_ERROR_MESSAGE.getValue(),
                        AttributeValue.newBuilder().setValue("e_msg 2").build()))
                .build());
    assertEquals("exc_msg", RpcSemanticConventionUtils.getGrpcErrorMsg(event));

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        OTelRpcSemanticConventions.RPC_SYSTEM.getValue(),
                        AttributeValue.newBuilder().setValue("grpc").build(),
                        RawSpanConstants.getValue(GRPC_ERROR_MESSAGE),
                        AttributeValue.newBuilder().setValue("e_msg 1").build(),
                        RPC_ERROR_MESSAGE.getValue(),
                        AttributeValue.newBuilder().setValue("e_msg 2").build()))
                .build());
    assertEquals("e_msg 2", RpcSemanticConventionUtils.getGrpcErrorMsg(event));

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        OTelRpcSemanticConventions.RPC_SYSTEM.getValue(),
                        AttributeValue.newBuilder().setValue("grpc").build(),
                        RawSpanConstants.getValue(GRPC_ERROR_MESSAGE),
                        AttributeValue.newBuilder().setValue("e_msg 1").build()))
                .build());
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

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RPC_REQUEST_BODY.getValue(),
                        AttributeValue.newBuilder().setValue("some rpc request body").build(),
                        "rpc.system",
                        AttributeValue.newBuilder().setValue("grpc").build()))
                .build());
    assertEquals(Optional.of(21), RpcSemanticConventionUtils.getGrpcRequestSize(event));
  }

  @Test
  public void testGetGrpcRequestSizePriority() {

    Map<String, AttributeValue> tagsMap =
        new HashMap<>() {
          {
            put(
                RawSpanConstants.getValue(ENVOY_REQUEST_SIZE),
                AttributeValue.newBuilder().setValue("1").build());
            put(
                RPC_REQUEST_METADATA_CONTENT_LENGTH.getValue(),
                AttributeValue.newBuilder().setValue("2").build());
            put(
                RawSpanConstants.getValue(GRPC_REQUEST_BODY),
                AttributeValue.newBuilder().setValue("some grpc request body").build());
            put(
                RPC_REQUEST_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc request body").build());
            put("rpc.system", AttributeValue.newBuilder().setValue("grpc").build());
          }
        };

    Event event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.of(1), RpcSemanticConventionUtils.getGrpcRequestSize(event));

    tagsMap =
        new HashMap<>() {
          {
            put(
                RPC_REQUEST_METADATA_CONTENT_LENGTH.getValue(),
                AttributeValue.newBuilder().setValue("2").build());
            put(
                RawSpanConstants.getValue(GRPC_REQUEST_BODY),
                AttributeValue.newBuilder().setValue("some grpc request body").build());
            put(
                RPC_REQUEST_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc request body").build());
            put("rpc.system", AttributeValue.newBuilder().setValue("grpc").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.of(2), RpcSemanticConventionUtils.getGrpcRequestSize(event));

    tagsMap =
        new HashMap<>() {
          {
            put(
                RawSpanConstants.getValue(GRPC_REQUEST_BODY),
                AttributeValue.newBuilder().setValue("some grpc request body").build());
            put(
                RPC_REQUEST_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc request body").build());
            put("rpc.system", AttributeValue.newBuilder().setValue("grpc").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.of(22), RpcSemanticConventionUtils.getGrpcRequestSize(event));

    tagsMap =
        new HashMap<>() {
          {
            put(
                RPC_REQUEST_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc request body").build());
            put("rpc.system", AttributeValue.newBuilder().setValue("grpc").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.of(21), RpcSemanticConventionUtils.getGrpcRequestSize(event));

    tagsMap =
        new HashMap<>() {
          {
            put(
                RPC_REQUEST_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc request body").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.empty(), RpcSemanticConventionUtils.getGrpcRequestSize(event));

    // test truncated grpc request body
    tagsMap =
        new HashMap<>() {
          {
            put(
                RawSpanConstants.getValue(GRPC_REQUEST_BODY),
                AttributeValue.newBuilder().setValue("some grpc request body").build());
            put(
                RawSpanConstants.getValue(GRPC_REQUEST_BODY_TRUNCATED),
                AttributeValue.newBuilder().setValue("true").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.empty(), RpcSemanticConventionUtils.getGrpcRequestSize(event));

    // test truncated rpc request body
    tagsMap =
        new HashMap<>() {
          {
            put(
                RPC_REQUEST_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc request body").build());
            put("rpc.system", AttributeValue.newBuilder().setValue("grpc").build());
            put(
                RPC_REQUEST_BODY_TRUNCATED.getValue(),
                AttributeValue.newBuilder().setValue("true").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.empty(), RpcSemanticConventionUtils.getGrpcRequestSize(event));
  }

  @Test
  public void testGetGrpcResponseSize() {
    Event event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(GRPC_RESPONSE_BODY), "some grpc response body");
    assertEquals(Optional.of(23), RpcSemanticConventionUtils.getGrpcResponseSize(event));

    event = mock(Event.class);
    assertTrue(RpcSemanticConventionUtils.getGrpcResponseSize(event).isEmpty());

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RPC_RESPONSE_BODY.getValue(),
                        AttributeValue.newBuilder().setValue("some rpc response body").build(),
                        "rpc.system",
                        AttributeValue.newBuilder().setValue("grpc").build()))
                .build());
    assertEquals(Optional.of(22), RpcSemanticConventionUtils.getGrpcResponseSize(event));
  }

  @Test
  public void testGetGrpcResponseSizePriority() {

    Map<String, AttributeValue> tagsMap =
        new HashMap<>() {
          {
            put(
                RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE),
                AttributeValue.newBuilder().setValue("1").build());
            put(
                RPC_RESPONSE_METADATA_CONTENT_LENGTH.getValue(),
                AttributeValue.newBuilder().setValue("2").build());
            put(
                RawSpanConstants.getValue(GRPC_RESPONSE_BODY),
                AttributeValue.newBuilder().setValue("some grpc response body").build());
            put(
                RPC_RESPONSE_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc response body").build());
            put("rpc.system", AttributeValue.newBuilder().setValue("grpc").build());
          }
        };

    Event event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.of(1), RpcSemanticConventionUtils.getGrpcResponseSize(event));

    tagsMap =
        new HashMap<>() {
          {
            put(
                RPC_RESPONSE_METADATA_CONTENT_LENGTH.getValue(),
                AttributeValue.newBuilder().setValue("2").build());
            put(
                RawSpanConstants.getValue(GRPC_RESPONSE_BODY),
                AttributeValue.newBuilder().setValue("some grpc response body").build());
            put(
                RPC_RESPONSE_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc response body").build());
            put("rpc.system", AttributeValue.newBuilder().setValue("grpc").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.of(2), RpcSemanticConventionUtils.getGrpcResponseSize(event));

    tagsMap =
        new HashMap<>() {
          {
            put(
                RawSpanConstants.getValue(GRPC_RESPONSE_BODY),
                AttributeValue.newBuilder().setValue("some grpc response body").build());
            put(
                RPC_RESPONSE_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc response body").build());
            put("rpc.system", AttributeValue.newBuilder().setValue("grpc").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.of(23), RpcSemanticConventionUtils.getGrpcResponseSize(event));

    tagsMap =
        new HashMap<>() {
          {
            put(
                RPC_RESPONSE_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc response body").build());
            put("rpc.system", AttributeValue.newBuilder().setValue("grpc").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.of(22), RpcSemanticConventionUtils.getGrpcResponseSize(event));

    tagsMap =
        new HashMap<>() {
          {
            put(
                RPC_RESPONSE_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc response body").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.empty(), RpcSemanticConventionUtils.getGrpcResponseSize(event));

    // test truncated grpc response body
    tagsMap =
        new HashMap<>() {
          {
            put(
                RawSpanConstants.getValue(GRPC_RESPONSE_BODY),
                AttributeValue.newBuilder().setValue("some grpc response body").build());
            put(
                RawSpanConstants.getValue(GRPC_RESPONSE_BODY_TRUNCATED),
                AttributeValue.newBuilder().setValue("true").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.empty(), RpcSemanticConventionUtils.getGrpcResponseSize(event));

    // test truncated rpc response body
    tagsMap =
        new HashMap<>() {
          {
            put(
                RPC_RESPONSE_BODY.getValue(),
                AttributeValue.newBuilder().setValue("some rpc response body").build());
            put("rpc.system", AttributeValue.newBuilder().setValue("grpc").build());
            put(
                RPC_RESPONSE_BODY_TRUNCATED.getValue(),
                AttributeValue.newBuilder().setValue("true").build());
          }
        };

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(tagsMap).build());
    assertEquals(Optional.empty(), RpcSemanticConventionUtils.getGrpcResponseSize(event));
  }

  @Test
  public void testGetGrpcURI() {
    Event e = mock(Event.class);

    // grpc_host_port is preferred
    Attributes attributes =
        buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Grpc.GRPC_HOST_PORT),
                "webhost:9011",
                "grpc.authority",
                "some-service:56003",
                "rpc.system",
                "grpc",
                OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
                "otel-host"));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = RpcSemanticConventionUtils.getGrpcURL(e);
    assertEquals("webhost:9011", v.get());

    // no relevant attributes
    attributes = buildAttributes(Map.of("span.kind", "client"));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcSemanticConventionUtils.getGrpcURL(e);
    assertTrue(v.isEmpty());

    // grpc authority is used
    attributes =
        buildAttributes(
            Map.of(
                "rpc.system",
                "grpc",
                OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
                "otel-host",
                "grpc.authority",
                "some-service:56003"));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcSemanticConventionUtils.getGrpcURL(e);
    assertEquals("some-service:56003", v.get());

    // rpc.system is not present but grpc authority is present
    attributes = buildAttributes(Map.of("grpc.authority", "some-service:56003"));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcSemanticConventionUtils.getGrpcURL(e);
    assertEquals("some-service:56003", v.get());

    // grpc authority is used
    attributes =
        buildAttributes(
            Map.of(
                RPC_REQUEST_METADATA_AUTHORITY.getValue(),
                "some-service:56003",
                "rpc.system",
                "grpc",
                OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
                "otel-host"));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcSemanticConventionUtils.getGrpcURL(e);
    assertEquals("some-service:56003", v.get());

    // rpc.system is not present but grpc authority is present
    attributes =
        buildAttributes(
            Map.of(
                "rpc.system",
                "grpc",
                RPC_REQUEST_METADATA_AUTHORITY.getValue(),
                "some-service:56003"));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcSemanticConventionUtils.getGrpcURL(e);
    assertEquals("some-service:56003", v.get());

    // otel host name is used
    attributes =
        buildAttributes(
            Map.of(
                "rpc.system",
                "grpc",
                OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
                "otel-host"));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcSemanticConventionUtils.getGrpcURL(e);
    assertEquals("otel-host", v.get());
  }

  @Test
  public void testGetGrpcAuthority() {
    Event e = mock(Event.class);

    Attributes attributes =
        buildAttributes(
            Map.of(
                "rpc.system", "grpc",
                "grpc.authority", "some-service:56003"));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = RpcSemanticConventionUtils.getSanitizedGrpcAuthority(e);
    assertEquals("some-service:56003", v.get());

    attributes =
        buildAttributes(
            Map.of(
                RPC_REQUEST_METADATA_AUTHORITY.getValue(),
                "some-service:56003",
                "rpc.system",
                "grpc",
                "grpc.authority",
                "other-service:56003"));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcSemanticConventionUtils.getSanitizedGrpcAuthority(e);
    assertEquals("some-service:56003", v.get());
  }

  @Test
  public void testGetSanitizedAuthorityValue() {
    // userinfo@host:port format, userinfo missing
    Optional<String> authority =
        RpcSemanticConventionUtils.getSanitizedAuthorityValue("some-service:56004");
    assertEquals("some-service:56004", authority.get());

    // userinfo@host:port format
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("ram@some-service:56004");
    assertEquals("some-service:56004", authority.get());

    // userinfo@host:port format
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("ram@www.example.com:56004");
    assertEquals("www.example.com:56004", authority.get());

    // userinfo@host:port format, userinfo missing
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("@www.example.com:56004");
    assertEquals("www.example.com:56004", authority.get());

    // userinfo@host:port format, port missing
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("ram@www.example.com:");
    assertEquals("www.example.com", authority.get());

    // userinfo@host:port format, port missing
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("ram@some-service");
    assertEquals("some-service", authority.get());

    // userinfo@host:port format, host missing
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("ram@:56004");
    assertFalse(authority.isPresent());

    // userinfo@host:port format, userinfo & port missing
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("some-service");
    assertEquals("some-service", authority.get());

    // url format, userinfo & path missing
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("http://some-service:56004");
    assertEquals("some-service:56004", authority.get());

    // url format, userinfo missing
    authority =
        RpcSemanticConventionUtils.getSanitizedAuthorityValue("https://some-service:56004/xyz");
    assertEquals("some-service:56004", authority.get());

    // url format
    authority =
        RpcSemanticConventionUtils.getSanitizedAuthorityValue("https://ram@some-service:56004/xyz");
    assertEquals("some-service:56004", authority.get());

    // url format, host missing
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("https://ram@:56004/xyz");
    assertTrue(authority.isEmpty());

    // url format, port missing
    authority =
        RpcSemanticConventionUtils.getSanitizedAuthorityValue("https://ram@some-service:/xyz");
    assertEquals("some-service", authority.get());

    // url format, userinfo & port missing
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("http://some-service/xyz");
    assertEquals("some-service", authority.get());

    // url format, port missing
    authority =
        RpcSemanticConventionUtils.getSanitizedAuthorityValue("http://ram@some-service/xyz");
    assertEquals("some-service", authority.get());

    // blank string
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("");
    assertTrue(authority.isEmpty());

    // userinfo@host:port, userinfo & port missing & host matches `localhost`
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("localhost");
    assertTrue(authority.isEmpty());

    // userinfo@host:port, userinfo  missing & host matches `localhost`
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("localhost:56004");
    assertTrue(authority.isEmpty());

    // userinfo@host:port, host matches `localhost`
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("ram@localhost:56004");
    assertTrue(authority.isEmpty());

    // userinfo@host:port, host missing
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue(":9000");
    assertTrue(authority.isEmpty());

    // url format, userinfo missing, host matches `localhost`
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("http://localhost:56004/xyz");
    assertTrue(authority.isEmpty());

    // url format, host matches `localhost`
    authority =
        RpcSemanticConventionUtils.getSanitizedAuthorityValue("http://ram@localhost:56004/xyz");
    assertTrue(authority.isEmpty());

    // url format, host is missing
    authority = RpcSemanticConventionUtils.getSanitizedAuthorityValue("http://:9000/path");
    assertTrue(authority.isEmpty());

    // invalid uri
    authority =
        RpcSemanticConventionUtils.getSanitizedAuthorityValue(
            "http://www.example.com/file[/].html");
    assertTrue(authority.isEmpty());

    // invalid uri
    authority =
        RpcSemanticConventionUtils.getSanitizedAuthorityValue(
            "http://ram@www.example.com/file[/].html");
    assertTrue(authority.isEmpty());
  }

  private static Attributes buildAttributes(Map<String, String> attributes) {
    return attributes.entrySet().stream()
        .collect(
            Collectors.collectingAndThen(
                Collectors.toMap(
                    Entry::getKey, entry -> AttributeValueCreator.create(entry.getValue())),
                map -> Attributes.newBuilder().setAttributeMap(map).build()));
  }
}
