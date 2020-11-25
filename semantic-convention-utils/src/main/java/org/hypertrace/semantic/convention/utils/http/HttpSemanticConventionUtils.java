package org.hypertrace.semantic.convention.utils.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.semantic.convention.utils.span.OTelSpanSemanticConventions;
import org.hypertrace.semantic.convention.utils.span.SpanSemanticConventionUtils;

/**
 * Utility class to fetch http span attributes
 */
public class HttpSemanticConventionUtils {

  // otel specific attributes
  private static final String OTEL_HTTP_METHOD = OTelHttpSemanticConventions.HTTP_METHOD.getValue();
  private static final String OTEL_HTTP_STATUS_CODE = OTelHttpSemanticConventions.HTTP_STATUS_CODE.getValue();

  private static final String OTHER_HTTP_METHOD = RawSpanConstants.getValue(Http.HTTP_METHOD);
  private static final String OTHER_HTTP_REQUEST_METHOD = RawSpanConstants.getValue(Http.HTTP_REQUEST_METHOD);
  private static final String[] OTHER_HTTP_STATUS_CODES =
      {
          RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE),
          RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_CODE)
      };
  private static final String OTHER_HTTP_RESPONSE_STATUS_MESSAGE =
      RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_MESSAGE);

  /**
   * @return attribute keys for http method
   */
  public static List<String> getAttributeKeysForHttpMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_HTTP_METHOD, OTEL_HTTP_METHOD));
  }

  /**
   * @return attribute keys for http request method
   */
  public static List<String> getAttributeKeysForHttpRequestMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_HTTP_REQUEST_METHOD));
  }

  public static Optional<String> getHttpUrlForOTelFormat(Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_URL.getValue())) {
      return Optional.of(attributeValueMap.get(OTelHttpSemanticConventions.HTTP_URL.getValue()).getValue());
    } else if (
        attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_SCHEME.getValue())
            && attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_HOST.getValue())
            && attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_TARGET.getValue())) {
      String url = String.format(
          "%s://%s%s",
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_SCHEME.getValue()),
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_HOST.getValue()),
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_TARGET.getValue()));
        return Optional.of(url);
    } else if (SpanSemanticConventionUtils.isClientSpanForOtelFormat(attributeValueMap)) {
      return getHttpUrlForOtelFormatClientSpan(attributeValueMap);
    } else if (SpanSemanticConventionUtils.isServerSpanForOtelFormat(attributeValueMap)) {
      return getHttpUrlForOtelFormatServerSpan(attributeValueMap);
    }
    return Optional.empty();
  }

  private static Optional<String> getHttpUrlForOtelFormatClientSpan(
      Map<String, AttributeValue> attributeValueMap) {
    if (
        attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_SCHEME.getValue())
            && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_NAME.getValue())
            && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())
            && attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_TARGET.getValue())) {
      String url = String.format(
          "%s://%s:%s%s",
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_SCHEME.getValue()),
          attributeValueMap.get(OTelSpanSemanticConventions.NET_PEER_NAME.getValue()),
          attributeValueMap.get(OTelSpanSemanticConventions.NET_PEER_PORT.getValue()),
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_TARGET.getValue()));
      return Optional.of(url);
    } else if (
        attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_SCHEME.getValue())
            && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_IP.getValue())
            && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())
            && attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_TARGET.getValue())) {
      String url = String.format(
          "%s://%s:%s%s",
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_SCHEME.getValue()),
          attributeValueMap.get(OTelSpanSemanticConventions.NET_PEER_IP.getValue()),
          attributeValueMap.get(OTelSpanSemanticConventions.NET_PEER_PORT.getValue()),
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_TARGET.getValue()));
      return Optional.of(url);
    }
    return Optional.empty();
  }

  private static Optional<String> getHttpUrlForOtelFormatServerSpan(
      Map<String, AttributeValue> attributeValueMap) {
    if (
        attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_SCHEME.getValue())
            && attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_SERVER_NAME.getValue())
            && attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_NET_HOST_PORT.getValue())
            && attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_TARGET.getValue())) {
      String url = String.format(
          "%s://%s:%s%s",
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_SCHEME.getValue()),
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_SERVER_NAME.getValue()),
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_NET_HOST_PORT.getValue()),
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_TARGET.getValue()));
      return Optional.of(url);
    } else if (
        attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_SCHEME.getValue())
            && attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_NET_HOST_NAME.getValue())
            && attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_NET_HOST_PORT.getValue())
            && attributeValueMap.containsKey(OTelHttpSemanticConventions.HTTP_TARGET.getValue())) {
      String url = String.format(
          "%s://%s:%s%s",
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_SCHEME.getValue()),
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_SERVER_NAME.getValue()),
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_NET_HOST_PORT.getValue()),
          attributeValueMap.get(OTelHttpSemanticConventions.HTTP_TARGET.getValue()));
      return Optional.of(url);
    }
    return Optional.empty();
  }
}
