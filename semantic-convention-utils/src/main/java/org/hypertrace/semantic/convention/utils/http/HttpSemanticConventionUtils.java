package org.hypertrace.semantic.convention.utils.http;

import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_HOST;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_NET_HOST_NAME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_NET_HOST_PORT;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_SCHEME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_SERVER_NAME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_TARGET;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_URL;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_METHOD;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_QUERY_STRING;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_DASH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_UNDERSCORE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_DOT_AGENT;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_METHOD;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_URL;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.util.StringUtils;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.semantic.convention.utils.span.SpanSemanticConventionUtils;

/** Utility class to fetch http span attributes */
public class HttpSemanticConventionUtils {

  // otel specific attributes
  private static final String OTEL_HTTP_METHOD = OTelHttpSemanticConventions.HTTP_METHOD.getValue();
  private static final String OTEL_HTTP_STATUS_CODE =
      OTelHttpSemanticConventions.HTTP_STATUS_CODE.getValue();

  private static final String OTHER_HTTP_METHOD = RawSpanConstants.getValue(Http.HTTP_METHOD);
  private static final String OTHER_HTTP_REQUEST_METHOD =
      RawSpanConstants.getValue(Http.HTTP_REQUEST_METHOD);
  private static final String[] OTHER_HTTP_STATUS_CODES = {
    RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE),
    RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_CODE)
  };
  private static final String OTHER_HTTP_RESPONSE_STATUS_MESSAGE =
      RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_MESSAGE);
  private static final String OTEL_HTTP_TARGET = OTelHttpSemanticConventions.HTTP_TARGET.getValue();
  private static final String RELATIVE_URL_CONTEXT = "http://hypertrace.org";

  private static final String SLASH = "/";

  private static final List<String> USER_AGENT_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(HTTP_USER_DOT_AGENT),
          RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE),
          RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH),
          RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER),
          RawSpanConstants.getValue(HTTP_USER_AGENT),
          OTelHttpSemanticConventions.HTTP_USER_AGENT.getValue());

  private static final List<String> HOST_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(Http.HTTP_HOST),
          OTelHttpSemanticConventions.HTTP_HOST.getValue());

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
          RawSpanConstants.getValue(Http.HTTP_URL),
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

  /** @return attribute keys for http method */
  public static List<String> getAttributeKeysForHttpMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_HTTP_METHOD, OTEL_HTTP_METHOD));
  }

  /** @return attribute keys for http request method */
  public static List<String> getAttributeKeysForHttpRequestMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_HTTP_REQUEST_METHOD));
  }

  public static List<String> getAttributeKeysForHttpTarget() {
    return Lists.newArrayList(Sets.newHashSet(OTEL_HTTP_TARGET));
  }

  /**
   * OTel mandates one of the following set to be present for http server span - http.scheme,
   * http.host, http.target - http.scheme, http.server_name, net.host.port, http.target -
   * http.scheme, net.host.name, net.host.port, http.target - http.url
   *
   * <p>http client span - http.url - http.scheme, http.host, http.target - http.scheme,
   * net.peer.name, net.peer.port, http.target - http.scheme, net.peer.ip, net.peer.port,
   * http.target
   *
   * <p>Reference:
   * https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/trace/semantic_conventions/http.md
   */
  public static Optional<String> getHttpUrlForOTelFormat(
      Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(HTTP_URL.getValue())) {
      return Optional.of(attributeValueMap.get(HTTP_URL.getValue()).getValue());
    } else if (attributeValueMap.containsKey(HTTP_SCHEME.getValue())
        && attributeValueMap.containsKey(HTTP_HOST.getValue())
        && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      String url =
          String.format(
              "%s://%s%s",
              attributeValueMap.get(HTTP_SCHEME.getValue()).getValue(),
              attributeValueMap.get(HTTP_HOST.getValue()).getValue(),
              attributeValueMap.get(HTTP_TARGET.getValue()).getValue());
      return Optional.of(url);
    } else if (SpanSemanticConventionUtils.isClientSpanForOtelFormat(attributeValueMap)
        || SpanSemanticConventionUtils.isClientSpanForOCFormat(attributeValueMap)) {
      return getHttpUrlForOtelFormatClientSpan(attributeValueMap);
    } else if (SpanSemanticConventionUtils.isServerSpanForOtelFormat(attributeValueMap)
        || SpanSemanticConventionUtils.isServerSpanForOCFormat(attributeValueMap)) {
      return getHttpUrlForOtelFormatServerSpan(attributeValueMap);
    }
    return Optional.empty();
  }

  private static Optional<String> getHttpUrlForOtelFormatClientSpan(
      Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(HTTP_SCHEME.getValue())
        && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_NAME.getValue())
        && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())
        && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      String url =
          String.format(
              "%s://%s:%s%s",
              attributeValueMap.get(HTTP_SCHEME.getValue()).getValue(),
              attributeValueMap
                  .get(OTelSpanSemanticConventions.NET_PEER_NAME.getValue())
                  .getValue(),
              attributeValueMap
                  .get(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())
                  .getValue(),
              attributeValueMap.get(HTTP_TARGET.getValue()).getValue());
      return Optional.of(url);
    } else if (attributeValueMap.containsKey(HTTP_SCHEME.getValue())
        && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_IP.getValue())
        && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())
        && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      String url =
          String.format(
              "%s://%s:%s%s",
              attributeValueMap.get(HTTP_SCHEME.getValue()).getValue(),
              attributeValueMap.get(OTelSpanSemanticConventions.NET_PEER_IP.getValue()).getValue(),
              attributeValueMap
                  .get(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())
                  .getValue(),
              attributeValueMap.get(HTTP_TARGET.getValue()).getValue());
      return Optional.of(url);
    }
    return Optional.empty();
  }

  private static Optional<String> getHttpUrlForOtelFormatServerSpan(
      Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(HTTP_SCHEME.getValue())
        && attributeValueMap.containsKey(HTTP_SERVER_NAME.getValue())
        && attributeValueMap.containsKey(HTTP_NET_HOST_PORT.getValue())
        && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      String url =
          String.format(
              "%s://%s:%s%s",
              attributeValueMap.get(HTTP_SCHEME.getValue()).getValue(),
              attributeValueMap.get(HTTP_SERVER_NAME.getValue()).getValue(),
              attributeValueMap.get(HTTP_NET_HOST_PORT.getValue()).getValue(),
              attributeValueMap.get(HTTP_TARGET.getValue()).getValue());
      return Optional.of(url);
    } else if (attributeValueMap.containsKey(HTTP_SCHEME.getValue())
        && attributeValueMap.containsKey(HTTP_NET_HOST_NAME.getValue())
        && attributeValueMap.containsKey(HTTP_NET_HOST_PORT.getValue())
        && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      String url =
          String.format(
              "%s://%s:%s%s",
              attributeValueMap.get(HTTP_SCHEME.getValue()).getValue(),
              attributeValueMap.get(HTTP_NET_HOST_NAME.getValue()).getValue(),
              attributeValueMap.get(HTTP_NET_HOST_PORT.getValue()).getValue(),
              attributeValueMap.get(HTTP_TARGET.getValue()).getValue());
      return Optional.of(url);
    }
    return Optional.empty();
  }

  public static Optional<AttributeValue> getValidHttpUrl(Event event) {
    for (String key : FULL_URL_ATTRIBUTES) {
      AttributeValue value = SpanAttributeUtils.getAttributeValue(event, key);
      if (value != null && isValidUrl(value.getValue())) {
        return Optional.of(value);
      }
    }
    return Optional.empty();
  }

  /**
   * accepts any absolute or relative URL. e.g. absolute URL:
   * http://hypertrace.org/customer?customer=392 relative URL: /customer?customer=392
   */
  public static boolean isValidUrl(String url) {
    try {
      getNormalizedUrl(url);
    } catch (MalformedURLException e) {
      return false;
    }
    return true;
  }

  public static URL getNormalizedUrl(String url) throws MalformedURLException {
    return new URL(new URL(RELATIVE_URL_CONTEXT), url);
  }

  public static Optional<String> getHttpUserAgent(Event event) {

    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String useragent : USER_AGENT_ATTRIBUTES) {
      if (attributeValueMap.get(useragent) != null
          && !StringUtils.isEmpty(attributeValueMap.get(useragent).getValue())) {
        return Optional.of(attributeValueMap.get(useragent).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpUserAgentFromHeader(Event event) {

    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    String userAgentKey = RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER);
    if (attributeValueMap.get(userAgentKey) != null
        && !StringUtils.isEmpty(attributeValueMap.get(userAgentKey).getValue())) {
      return Optional.of(attributeValueMap.get(userAgentKey).getValue());
    }

    return Optional.empty();
  }

  public static Optional<String> getHttpHost(Event event) {

    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String host : HOST_ATTRIBUTES) {
      if (attributeValueMap.get(host) != null) {
        return Optional.of(attributeValueMap.get(host).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpPath(Event event) {

    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String path : URL_PATH_ATTRIBUTES) {
      if (attributeValueMap.get(path) != null) {
        String s = attributeValueMap.get(path).getValue();
        if (StringUtils.isNotBlank(s) && s.startsWith(SLASH)) {
          return getPathFromUrlObject(s).map(str -> removeTrailingSlash(str));
        }
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpMethod(Event event) {

    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String method : METHOD_ATTRIBUTES) {
      if (attributeValueMap.get(method) != null
          && !StringUtils.isBlank(attributeValueMap.get(method).getValue())) {
        return Optional.of(attributeValueMap.get(method).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpScheme(Event event) {

    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String scheme : SCHEME_ATTRIBUTES) {
      if (attributeValueMap.get(scheme) != null
          && !StringUtils.isEmpty(attributeValueMap.get(scheme).getValue())) {
        return Optional.of(attributeValueMap.get(scheme).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpUrl(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String url : FULL_URL_ATTRIBUTES) {
      if (attributeValueMap.get(url) != null) {
        String urlVal = attributeValueMap.get(url).getValue();
        if (!StringUtils.isBlank(urlVal) && isValidUrl(urlVal)) {
          if (isAbsoluteUrl(urlVal)) {
            return Optional.of(urlVal);
          } else {
            Optional<String> url2 = getHttpUrlForOTelFormat(attributeValueMap);
            return url2.isPresent() ? url2 : Optional.of(urlVal);
          }
        }
      }
    }

    return getHttpUrlForOTelFormat(attributeValueMap);
  }

  public static Optional<String> getHttpQueryString(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String query_string : QUERY_STRING_ATTRIBUTES) {
      if (attributeValueMap.get(query_string) != null) {
        return Optional.of(attributeValueMap.get(query_string).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<Integer> getHttpRequestSize(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String reqsa : REQUEST_SIZE_ATTRIBUTES) {
      if (attributeValueMap.get(reqsa) != null) {
        return Optional.of(Integer.parseInt(attributeValueMap.get(reqsa).getValue()));
      }
    }
    return Optional.empty();
  }

  public static Optional<Integer> getHttpResponseSize(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String rsa : RESPONSE_SIZE_ATTRIBUTES) {
      if (attributeValueMap.get(rsa) != null) {
        return Optional.of(Integer.parseInt(attributeValueMap.get(rsa).getValue()));
      }
    }
    return Optional.empty();
  }

  private static Optional<String> getPathFromUrlObject(String urlPath) {
    try {
      URL url = getNormalizedUrl(urlPath);
      return Optional.of(url.getPath());
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }
    return Optional.empty();
  }

  private static String removeTrailingSlash(String s) {
    // Ends with "/" and it's not home page path
    return s.endsWith(SLASH) && s.length() > 1 ? s.substring(0, s.length() - 1) : s;
  }

  private static boolean isAbsoluteUrl(String urlStr) {
    try {
      URL url = getNormalizedUrl(urlStr);
      return url.toString().equals(urlStr);
    } catch (MalformedURLException e) {
      // ignore
    }
    return false;
  }
}
