package org.hypertrace.semantic.convention.utils.http;

import static org.hypertrace.core.span.constants.v1.Http.*;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_METHOD;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;

public class HttpMigration {

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
      if ((attributeValueMap.get(path) != null) && ("" != attributeValueMap.get(path).getValue())) {
        return Optional.of(attributeValueMap.get(path).getValue());
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
}
