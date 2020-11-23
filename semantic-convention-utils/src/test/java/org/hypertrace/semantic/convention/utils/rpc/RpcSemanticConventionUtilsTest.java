package org.hypertrace.semantic.convention.utils.rpc;

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
import org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link RpcSemanticConventionUtils}
 */
class RpcSemanticConventionUtilsTest {

  @Test
  public void testIsRpcTypeGrpcForOTelFormat() {
    Event e = mock(Event.class);
    // otel format
    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelRpcSemanticConventions.RPC_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(OTelRpcSemanticConventions.RPC_SYSTEM_VALUE_GRPC.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = RpcSemanticConventionUtils.isRpcTypeGrpcForOTelFormat(e);
    assertTrue(v);

    attributes = SemanticConventionTestUtil.buildAttributes(
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

    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Grpc.GRPC_HOST_PORT),
            SemanticConventionTestUtil.buildAttributeValue("webhost:9011")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = RpcSemanticConventionUtils.getGrpcURI(e);
    assertEquals("webhost:9011", v.get());

    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            "span.kind",
            SemanticConventionTestUtil.buildAttributeValue("client")));
    when(e.getAttributes()).thenReturn(attributes);
    v = RpcSemanticConventionUtils.getGrpcURI(e);
    assertTrue(v.isEmpty());
  }
}