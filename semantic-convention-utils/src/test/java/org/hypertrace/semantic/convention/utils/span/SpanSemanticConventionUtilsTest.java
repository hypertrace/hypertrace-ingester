package org.hypertrace.semantic.convention.utils.span;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link SpanSemanticConventionUtils}
 */
public class SpanSemanticConventionUtilsTest {

  @Test
  public void testGetURIForOtelFormat() {
    Event e = mock(Event.class);
    // host present
    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("example.com")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = SpanSemanticConventionUtils.getURIForOtelFormat(e);
    assertEquals("example.com", v.get());

    // ip present
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelSpanSemanticConventions.NET_PEER_IP.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("172.0.1.17")));
    when(e.getAttributes()).thenReturn(attributes);
    v = SpanSemanticConventionUtils.getURIForOtelFormat(e);
    assertEquals("172.0.1.17", v.get());

    // host & port
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelSpanSemanticConventions.NET_PEER_IP.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("172.0.1.17"),
            OTelSpanSemanticConventions.NET_PEER_PORT.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("2705")));
    when(e.getAttributes()).thenReturn(attributes);
    v = SpanSemanticConventionUtils.getURIForOtelFormat(e);
    assertEquals("172.0.1.17:2705", v.get());

    // ip & host both present
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelSpanSemanticConventions.NET_PEER_IP.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("172.0.1.17"),
            OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("example.com"),
            OTelSpanSemanticConventions.NET_PEER_PORT.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("2705")));
    when(e.getAttributes()).thenReturn(attributes);
    v = SpanSemanticConventionUtils.getURIForOtelFormat(e);
    assertEquals("example.com:2705", v.get());

    // empty host
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelSpanSemanticConventions.NET_PEER_IP.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(""),
            OTelSpanSemanticConventions.NET_PEER_PORT.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("2705")));
    when(e.getAttributes()).thenReturn(attributes);
    v = SpanSemanticConventionUtils.getURIForOtelFormat(e);
    assertFalse(v.isPresent());
  }
}
