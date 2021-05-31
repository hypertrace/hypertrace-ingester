package org.hypertrace.semantic.convention.utils.http;

import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.*;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_METHOD;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_URL;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

import io.micrometer.core.instrument.util.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;

public class HttpMigration {

  private static final String SLASH = "/";

  private static final List<String> USER_AGENT_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(HTTP_USER_DOT_AGENT),
          RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE),
          RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH),
          RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER),
          RawSpanConstants.getValue(HTTP_USER_AGENT),
          OTelHttpSemanticConventions.HTTP_USER_AGENT.getValue());

  private static final List<String> HOST_ATTRIBUTES = List.of(RawSpanConstants.getValue(HTTP_HOST));

  private static final List<String> URL_PATH_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(HTTP_REQUEST_PATH),
          RawSpanConstants.getValue(HTTP_PATH),
          OTelHttpSemanticConventions.HTTP_TARGET.getValue());

  private static final List<String> METHOD_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(HTTP_REQUEST_METHOD),
          RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD),
          OTelHttpSemanticConventions.HTTP_METHOD.getValue());

  private static final List<String> SCHEME_ATTRIBUTES =
      List.of(OTelHttpSemanticConventions.HTTP_SCHEME.getValue());

  private static final List<String> FULL_URL_ATTRIBUTES =
          List.of(
                  RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL),
                  RawSpanConstants.getValue(HTTP_REQUEST_URL),
                  RawSpanConstants.getValue(HTTP_URL),
                  OTelHttpSemanticConventions.HTTP_URL.getValue());

  private static final List<String> QUERY_STRING_ATTRIBUTES =
          List.of(RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING));

  private static final List<String> REQUEST_SIZE_ATTRIBUTES =
          List.of(
                  RawSpanConstants.getValue(ENVOY_REQUEST_SIZE),
                  RawSpanConstants.getValue(HTTP_REQUEST_SIZE),
                  OTelHttpSemanticConventions.HTTP_REQUEST_SIZE.getValue());

  private static final List<String> RESPONSE_SIZE_ATTRIBUTES =
          List.of(
                  RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE),
                  RawSpanConstants.getValue(HTTP_RESPONSE_SIZE),
                  OTelHttpSemanticConventions.HTTP_RESPONSE_SIZE.getValue());

  public static Optional<String> getHttpUserAgent(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String useragent : USER_AGENT_ATTRIBUTES) {
      if ((attributeValueMap.get(useragent) != null)
          && ("" != attributeValueMap.get(useragent).getValue())) {
        return Optional.of(attributeValueMap.get(useragent).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpHost(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String host : HOST_ATTRIBUTES) {
      if ((attributeValueMap.get(host) != null) && ("" != attributeValueMap.get(host).getValue())) {
        return Optional.of(attributeValueMap.get(host).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpPath(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String path : URL_PATH_ATTRIBUTES) {
      if (attributeValueMap.get(path) != null) {
        String s = attributeValueMap.get(path).getValue();
        if (StringUtils.isNotBlank(s) && s.startsWith(SLASH)) {
          return Optional.of(s);
        }
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpMethod(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String method : METHOD_ATTRIBUTES) {
      if ((attributeValueMap.get(method) != null)
          && ("" != attributeValueMap.get(method).getValue())) {
        return Optional.of(attributeValueMap.get(method).getValue());
      }
    }
    return Optional.empty();
  }

  @Nullable
  public static Optional<String> getHttpScheme(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String scheme : SCHEME_ATTRIBUTES) {
      if ((attributeValueMap.get(scheme) != null)
          && ("" != attributeValueMap.get(scheme).getValue())) {
        return Optional.of(attributeValueMap.get(scheme).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpUrl(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String url : FULL_URL_ATTRIBUTES) {
      if ((attributeValueMap.get(url) != null) && ("" != attributeValueMap.get(url).getValue())) {
        return Optional.of(attributeValueMap.get(url).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpQueryString(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String query_string : QUERY_STRING_ATTRIBUTES) {
      if ((attributeValueMap.get(query_string) != null)
              && ("" != attributeValueMap.get(query_string).getValue())) {
        return Optional.of(attributeValueMap.get(query_string).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<Integer> getHttpRequestSize(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String reqsa : REQUEST_SIZE_ATTRIBUTES) {
      if ((attributeValueMap.get(reqsa) != null)
              && ("" != attributeValueMap.get(reqsa).getValue())) {
        return Optional.of(Integer.parseInt(attributeValueMap.get(reqsa).getValue()));
      }
    }
    return Optional.empty();
  }

  public static Optional<Integer> getHttpResponseSize(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String rsa : RESPONSE_SIZE_ATTRIBUTES) {
      if ((attributeValueMap.get(rsa) != null) && ("" != attributeValueMap.get(rsa).getValue())) {
        return Optional.of(Integer.parseInt(attributeValueMap.get(rsa).getValue()));
      }
    }
    return Optional.empty();
  }
}
