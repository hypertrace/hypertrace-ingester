package org.hypertrace.telemetry.attribute.utils.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Grpc;
import org.hypertrace.telemetry.attribute.utils.AttributeTestUtil;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link RpcTelemetryAttributeUtils}
 */
class RpcTelemetryAttributeUtilsTest {

  @Test
  public void testIsRpcTypeGrpcForOTelFormat() {
    Event e = mock(Event.class);
    // otel format
    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelRpcAttributes.RPC_SYSTEM.getValue(),
            AttributeTestUtil.buildAttributeValue(OTelRpcAttributes.RPC_SYSTEM_VALUE_GRPC.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = RpcTelemetryAttributeUtils.isRpcTypeGrpcForOTelFormat(e);
    assertTrue(v);

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelRpcAttributes.RPC_SYSTEM.getValue(),
            AttributeTestUtil.buildAttributeValue("other")));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcTelemetryAttributeUtils.isRpcTypeGrpcForOTelFormat(e);
    assertFalse(v);
  }

  @Test
  void getGrpcURI() {
    Event e = mock(Event.class);

    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Grpc.GRPC_HOST_PORT),
            AttributeTestUtil.buildAttributeValue("webhost:9011")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = RpcTelemetryAttributeUtils.getGrpcURI(e);
    assertEquals("webhost:9011", v.get());

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            "span.kind",
            AttributeTestUtil.buildAttributeValue("client")));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcTelemetryAttributeUtils.getGrpcURI(e);
    assertTrue(v.isEmpty());
  }
}