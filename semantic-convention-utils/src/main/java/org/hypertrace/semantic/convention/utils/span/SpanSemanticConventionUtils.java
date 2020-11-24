package org.hypertrace.semantic.convention.utils.span;

import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;

/**
 * Utility to read span attributes
 */
public class SpanSemanticConventionUtils {

  private static final String OTEL_NET_PEER_IP = OTelSpanSemanticConventions.NET_PEER_IP.getValue();
  private static final String OTEL_NET_PEER_PORT = OTelSpanSemanticConventions.NET_PEER_PORT.getValue();
  private static final String OTEL_NET_PEER_NAME = OTelSpanSemanticConventions.NET_PEER_NAME.getValue();

  /**
   * @param event Object encapsulating span data
   * @return URI based on OTel format
   */
  public static Optional<String> getURIForOtelFormat(Event event) {
    String host = SpanAttributeUtils.getStringAttributeWithDefault(
        event, OTEL_NET_PEER_NAME,
        SpanAttributeUtils.getStringAttribute(event, OTEL_NET_PEER_IP));
    if (StringUtils.isBlank(host)) {
      return Optional.empty();
    }
    if (SpanAttributeUtils.containsAttributeKey(event, OTEL_NET_PEER_PORT)) {
      return Optional.of(String.format(
          "%s:%s", host, SpanAttributeUtils.getStringAttribute(event, OTEL_NET_PEER_PORT)));
    }
    return Optional.of(host);
  }

  /**
   * @param attributeValueMap map of span data and attribute value
   * @return URI based on OTel format
   */
  public static Optional<String> getURIForOtelFormat(Map<String, AttributeValue> attributeValueMap) {
    AttributeValue hostAttribute = attributeValueMap.getOrDefault(
        OTEL_NET_PEER_NAME,
        attributeValueMap.get(OTEL_NET_PEER_IP));
    if (null == hostAttribute || StringUtils.isBlank(hostAttribute.getValue())) {
      return Optional.empty();
    }
    if (attributeValueMap.containsKey(OTEL_NET_PEER_PORT)
        && !StringUtils.isBlank(attributeValueMap.get(OTEL_NET_PEER_PORT).getValue())) {
      return Optional.of(String.format(
          "%s:%s", hostAttribute.getValue(),
          attributeValueMap.get(OTEL_NET_PEER_PORT).getValue()));
    }
    return Optional.of(hostAttribute.getValue());
  }
}
