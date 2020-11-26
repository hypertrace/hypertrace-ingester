package org.hypertrace.semantic.convention.utils.http;

import static org.hypertrace.semantic.convention.utils.http.OTelHttpSemanticConventions.HTTP_HOST;
import static org.hypertrace.semantic.convention.utils.http.OTelHttpSemanticConventions.HTTP_NET_HOST_NAME;
import static org.hypertrace.semantic.convention.utils.http.OTelHttpSemanticConventions.HTTP_NET_HOST_PORT;
import static org.hypertrace.semantic.convention.utils.http.OTelHttpSemanticConventions.HTTP_SCHEME;
import static org.hypertrace.semantic.convention.utils.http.OTelHttpSemanticConventions.HTTP_SERVER_NAME;
import static org.hypertrace.semantic.convention.utils.http.OTelHttpSemanticConventions.HTTP_TARGET;
import static org.hypertrace.semantic.convention.utils.http.OTelHttpSemanticConventions.HTTP_URL;

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

  /**
   * OTel mandates one of the following set to be present for
   * http server span
   * - http.scheme, http.host, http.target
   * - http.scheme, http.server_name, net.host.port, http.target
   * - http.scheme, net.host.name, net.host.port, http.target
   * - http.url
   *
   * http client span
   * - http.url
   * - http.scheme, http.host, http.target
   * - http.scheme, net.peer.name, net.peer.port, http.target
   * - http.scheme, net.peer.ip, net.peer.port, http.target
   *
   * Reference: https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/trace/semantic_conventions/http.md
   */
  public static Optional<String> getHttpUrlForOTelFormat(Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(HTTP_URL.getValue())) {
      return Optional.of(attributeValueMap.get(HTTP_URL.getValue()).getValue());
    } else if (
        attributeValueMap.containsKey(HTTP_SCHEME.getValue())
            && attributeValueMap.containsKey(HTTP_HOST.getValue())
            && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      String url = String.format(
          "%s://%s%s",
          attributeValueMap.get(HTTP_SCHEME.getValue()).getValue(),
          attributeValueMap.get(HTTP_HOST.getValue()).getValue(),
          attributeValueMap.get(HTTP_TARGET.getValue()).getValue());
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
        attributeValueMap.containsKey(HTTP_SCHEME.getValue())
            && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_NAME.getValue())
            && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())
            && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      String url = String.format(
          "%s://%s:%s%s",
          attributeValueMap.get(HTTP_SCHEME.getValue()).getValue(),
          attributeValueMap.get(OTelSpanSemanticConventions.NET_PEER_NAME.getValue()).getValue(),
          attributeValueMap.get(OTelSpanSemanticConventions.NET_PEER_PORT.getValue()).getValue(),
          attributeValueMap.get(HTTP_TARGET.getValue()).getValue());
      return Optional.of(url);
    } else if (
        attributeValueMap.containsKey(HTTP_SCHEME.getValue())
            && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_IP.getValue())
            && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())
            && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      String url = String.format(
          "%s://%s:%s%s",
          attributeValueMap.get(HTTP_SCHEME.getValue()).getValue(),
          attributeValueMap.get(OTelSpanSemanticConventions.NET_PEER_IP.getValue()).getValue(),
          attributeValueMap.get(OTelSpanSemanticConventions.NET_PEER_PORT.getValue()).getValue(),
          attributeValueMap.get(HTTP_TARGET.getValue()).getValue());
      return Optional.of(url);
    }
    return Optional.empty();
  }

  private static Optional<String> getHttpUrlForOtelFormatServerSpan(
      Map<String, AttributeValue> attributeValueMap) {
    if (
        attributeValueMap.containsKey(HTTP_SCHEME.getValue())
            && attributeValueMap.containsKey(HTTP_SERVER_NAME.getValue())
            && attributeValueMap.containsKey(HTTP_NET_HOST_PORT.getValue())
            && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      String url = String.format(
          "%s://%s:%s%s",
          attributeValueMap.get(HTTP_SCHEME.getValue()).getValue(),
          attributeValueMap.get(HTTP_SERVER_NAME.getValue()).getValue(),
          attributeValueMap.get(HTTP_NET_HOST_PORT.getValue()).getValue(),
          attributeValueMap.get(HTTP_TARGET.getValue()).getValue());
      return Optional.of(url);
    } else if (
        attributeValueMap.containsKey(HTTP_SCHEME.getValue())
            && attributeValueMap.containsKey(HTTP_NET_HOST_NAME.getValue())
            && attributeValueMap.containsKey(HTTP_NET_HOST_PORT.getValue())
            && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      String url = String.format(
          "%s://%s:%s%s",
          attributeValueMap.get(HTTP_SCHEME.getValue()).getValue(),
          attributeValueMap.get(HTTP_NET_HOST_NAME.getValue()).getValue(),
          attributeValueMap.get(HTTP_NET_HOST_PORT.getValue()).getValue(),
          attributeValueMap.get(HTTP_TARGET.getValue()).getValue());
      return Optional.of(url);
    }
    return Optional.empty();
  }
}
