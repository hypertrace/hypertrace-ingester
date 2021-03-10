package org.hypertrace.semantic.convention.utils.span;

import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OpenTracingSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.OCAttribute;
import org.hypertrace.core.span.constants.v1.OCSpanKind;

/** Utility to read span attributes */
public class SpanSemanticConventionUtils {

  private static final String OTEL_NET_PEER_IP = OTelSpanSemanticConventions.NET_PEER_IP.getValue();
  private static final String OTEL_NET_PEER_PORT =
      OTelSpanSemanticConventions.NET_PEER_PORT.getValue();
  private static final String OTEL_NET_PEER_NAME =
      OTelSpanSemanticConventions.NET_PEER_NAME.getValue();

  private static final String OT_PEER_HOSTNAME =
      OpenTracingSpanSemanticConventions.PEER_HOSTNAME.getValue();
  private static final String OT_PEER_IP = OpenTracingSpanSemanticConventions.PEER_IPV4.getValue();
  private static final String OT_PEER_PORT =
      OpenTracingSpanSemanticConventions.PEER_PORT.getValue();

  /**
   * @param event Object encapsulating span data
   * @return URI based on OTel format
   */
  public static Optional<String> getURIForOtelFormat(Event event) {
    String host =
        SpanAttributeUtils.getStringAttributeWithDefault(
            event,
            OTEL_NET_PEER_NAME,
            SpanAttributeUtils.getStringAttribute(event, OTEL_NET_PEER_IP));
    if (StringUtils.isBlank(host)) {
      return Optional.empty();
    }
    if (SpanAttributeUtils.containsAttributeKey(event, OTEL_NET_PEER_PORT)) {
      return Optional.of(
          String.format(
              "%s:%s", host, SpanAttributeUtils.getStringAttribute(event, OTEL_NET_PEER_PORT)));
    }
    return Optional.of(host);
  }

  /**
   * @param attributeValueMap map of span data and attribute value
   * @return URI based on OTel format
   */
  public static Optional<String> getURIForOtelFormat(
      Map<String, AttributeValue> attributeValueMap) {
    AttributeValue hostAttribute =
        attributeValueMap.getOrDefault(OTEL_NET_PEER_NAME, attributeValueMap.get(OTEL_NET_PEER_IP));
    if (null == hostAttribute || StringUtils.isBlank(hostAttribute.getValue())) {
      return Optional.empty();
    }
    if (attributeValueMap.containsKey(OTEL_NET_PEER_PORT)
        && !StringUtils.isBlank(attributeValueMap.get(OTEL_NET_PEER_PORT).getValue())) {
      return Optional.of(
          String.format(
              "%s:%s",
              hostAttribute.getValue(), attributeValueMap.get(OTEL_NET_PEER_PORT).getValue()));
    }
    return Optional.of(hostAttribute.getValue());
  }

  /**
   * @param event Object encapsulating span data
   * @return URI based on Open Tracing format
   */
  public static Optional<String> getURIforOpenTracingFormat(Event event) {
    String host =
        SpanAttributeUtils.getStringAttributeWithDefault(
            event, OT_PEER_HOSTNAME, SpanAttributeUtils.getStringAttribute(event, OT_PEER_IP));
    if (StringUtils.isBlank(host)) {
      return Optional.empty();
    }

    if (SpanAttributeUtils.containsAttributeKey(event, OT_PEER_PORT)) {
      return Optional.of(
          String.format("%s:%s", host, SpanAttributeUtils.getStringAttribute(event, OT_PEER_PORT)));
    }
    return Optional.of(host);
  }

  /**
   * @param attributeValueMap map of span data and attribute value
   * @return URI based on Open Tracing format
   */
  public static boolean isClientSpanForOtelFormat(Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(OTelSpanSemanticConventions.SPAN_KIND.getValue())) {
      return OTelSpanSemanticConventions.SPAN_KIND_CLIENT_VALUE
          .getValue()
          .equalsIgnoreCase(
              attributeValueMap.get(OTelSpanSemanticConventions.SPAN_KIND.getValue()).getValue());
    }
    return false;
  }

  public static boolean isServerSpanForOtelFormat(Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(OTelSpanSemanticConventions.SPAN_KIND.getValue())) {
      return OTelSpanSemanticConventions.SPAN_KIND_SERVER_VALUE
          .getValue()
          .equalsIgnoreCase(
              attributeValueMap.get(OTelSpanSemanticConventions.SPAN_KIND.getValue()).getValue());
    }
    return false;
  }

  public static boolean isClientSpanForOCFormat(Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(
        RawSpanConstants.getValue(OCAttribute.OC_ATTRIBUTE_SPAN_KIND))) {
      return RawSpanConstants.getValue(OCSpanKind.OC_SPAN_KIND_CLIENT)
          .equalsIgnoreCase(
              attributeValueMap
                  .get(RawSpanConstants.getValue(OCAttribute.OC_ATTRIBUTE_SPAN_KIND))
                  .getValue());
    }
    return false;
  }

  public static boolean isServerSpanForOCFormat(Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(
        RawSpanConstants.getValue(OCAttribute.OC_ATTRIBUTE_SPAN_KIND))) {
      return RawSpanConstants.getValue(OCSpanKind.OC_SPAN_KIND_SERVER)
          .equalsIgnoreCase(
              attributeValueMap
                  .get(RawSpanConstants.getValue(OCAttribute.OC_ATTRIBUTE_SPAN_KIND))
                  .getValue());
    }
    return false;
  }
}
