package org.hypertrace.telemetry.attribute.utils.span;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.telemetry.attribute.utils.AttributeTestUtil;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link SpanTelemetryAttributeUtils}
 */
public class SpanTelemetryAttributeUtilsTest {

  @Test
  public void testGetURIForOtelFormat() {
    Event e = mock(Event.class);
    // host present
    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelSpanAttributes.NET_PEER_NAME.getValue(),
            AttributeTestUtil.buildAttributeValue("example.com")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = SpanTelemetryAttributeUtils.getURIForOtelFormat(e);
    assertEquals("example.com", v.get());

    // ip present
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelSpanAttributes.NET_PEER_IP.getValue(),
            AttributeTestUtil.buildAttributeValue("172.0.1.17")));
    when(e.getAttributes()).thenReturn(attributes);
    v = SpanTelemetryAttributeUtils.getURIForOtelFormat(e);
    assertEquals("172.0.1.17", v.get());

    // host & port
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelSpanAttributes.NET_PEER_IP.getValue(),
            AttributeTestUtil.buildAttributeValue("172.0.1.17"),
            OTelSpanAttributes.NET_PEER_PORT.getValue(),
            AttributeTestUtil.buildAttributeValue("2705")));
    when(e.getAttributes()).thenReturn(attributes);
    v = SpanTelemetryAttributeUtils.getURIForOtelFormat(e);
    assertEquals("172.0.1.17:2705", v.get());

    // empty host
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelSpanAttributes.NET_PEER_IP.getValue(),
            AttributeTestUtil.buildAttributeValue(""),
            OTelSpanAttributes.NET_PEER_PORT.getValue(),
            AttributeTestUtil.buildAttributeValue("2705")));
    when(e.getAttributes()).thenReturn(attributes);
    v = SpanTelemetryAttributeUtils.getURIForOtelFormat(e);
    assertFalse(v.isPresent());
  }
}
