package org.hypertrace.semantic.convention.utils.http;

import static com.google.common.net.HttpHeaders.COOKIE;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static org.hypertrace.core.semantic.convention.constants.http.HttpSemanticConventions.HTTP_REQUEST_FORWARDED;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_HOST;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_NET_HOST_NAME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_NET_HOST_PORT;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_SCHEME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_SERVER_NAME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_TARGET;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_URL;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HTTP_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HTTP_RESPONSE_BODY;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_BODY_TRUNCATED;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_CONTENT_LENGTH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_CONTENT_TYPE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_METHOD;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_QUERY_STRING;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_X_FORWARDED_FOR_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_BODY_TRUNCATED;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_CONTENT_LENGTH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_CONTENT_TYPE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_DASH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_UNDERSCORE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_DOT_AGENT;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_METHOD;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_URL;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.InternetDomainName;
import java.net.HttpCookie;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.reflect.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.semantic.convention.constants.deployment.OTelDeploymentSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.http.HttpSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.semantic.convention.utils.span.SpanSemanticConventionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to fetch http span attributes */
public class HttpSemanticConventionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpSemanticConventionUtils.class);

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

  private static final String HTTP_REQUEST_BODY = RawSpanConstants.getValue(HTTP_HTTP_REQUEST_BODY);
  private static final String HTTP_REQUEST_BODY_TRUNCATED_ATTR =
      RawSpanConstants.getValue(HTTP_REQUEST_BODY_TRUNCATED);
  private static final String HTTP_RESPONSE_BODY =
      RawSpanConstants.getValue(HTTP_HTTP_RESPONSE_BODY);
  private static final String HTTP_RESPONSE_BODY_TRUNCATED_ATTR =
      RawSpanConstants.getValue(HTTP_RESPONSE_BODY_TRUNCATED);

  private static final String DOT = ".";
  private static final String HTTP_REQUEST_HEADER_PREFIX =
      RawSpanConstants.getValue(HTTP_REQUEST_HEADER) + DOT;
  private static final String REQUEST_COOKIE_HEADER_KEY =
      HTTP_REQUEST_HEADER_PREFIX + COOKIE.toLowerCase();
  private static final Splitter SEMICOLON_SPLITTER =
      Splitter.on(";").trimResults().omitEmptyStrings();
  private static final Splitter COOKIE_KEY_VALUE_SPLITTER =
      Splitter.on("=").limit(2).trimResults().omitEmptyStrings();

  private static final String SLASH = "/";

  private static final String HTTP_REQUEST_FORWARDED_ATTRIBUTE = HTTP_REQUEST_FORWARDED.getValue();
  private static final String HTTP_REQUEST_ORIGIN =
      HttpSemanticConventions.HTTP_REQUEST_ORIGIN.getValue();
  private static final String HTTP_RESPONSE_HEADER_PREFIX =
      RawSpanConstants.getValue(HTTP_RESPONSE_HEADER) + DOT;
  public static final String RESPONSE_COOKIE_HEADER_PREFIX =
      HTTP_RESPONSE_HEADER_PREFIX + SET_COOKIE.toLowerCase();

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
      List.of(
          HttpSemanticConventions.HTTP_REQUEST_X_FORWARDED_PROTO.getValue(),
          OTelHttpSemanticConventions.HTTP_SCHEME.getValue());

  public static final List<String> FULL_URL_ATTRIBUTES =
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
          OTelHttpSemanticConventions.HTTP_REQUEST_SIZE.getValue(),
          RawSpanConstants.getValue(HTTP_REQUEST_CONTENT_LENGTH));

  private static final List<String> RESPONSE_SIZE_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE),
          RawSpanConstants.getValue(HTTP_RESPONSE_SIZE),
          OTelHttpSemanticConventions.HTTP_RESPONSE_SIZE.getValue(),
          RawSpanConstants.getValue(HTTP_RESPONSE_CONTENT_LENGTH));

  private static final List<String> STATUS_CODE_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_STATUS_CODE),
          RawSpanConstants.getValue(HTTP_RESPONSE_STATUS_CODE),
          OTelHttpSemanticConventions.HTTP_STATUS_CODE.getValue());

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

  public static List<String> getAttributeKeysForHttpTarget() {
    return Lists.newArrayList(Sets.newHashSet(OTEL_HTTP_TARGET));
  }

  public static Optional<String> getFullHttpUrl(Event event) {
    Optional<String> fullHttpUrl = getHttpUrl(event);
    return fullHttpUrl.isPresent()
        ? fullHttpUrl
        : getValidHttpUrl(event).map(AttributeValue::getValue);
  }

  public static Optional<String> getDestinationIpAddress(Event event) {
    return Optional.ofNullable(
            SpanAttributeUtils.getStringAttribute(
                event, OTelSpanSemanticConventions.NET_SOCK_PEER_ADDR.getValue()))
        .or(
            () ->
                Optional.ofNullable(
                    SpanAttributeUtils.getStringAttribute(
                        event, OTelSpanSemanticConventions.NET_PEER_IP.getValue())));
  }

  public static Optional<String> getEnvironmentForSpan(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getStringAttribute(
            event, OTelDeploymentSemanticConventions.DEPLOYMENT_ENVIRONMENT.getValue()));
  }

  public static String getPrimaryDomain(String host) {
    try {
      return InternetDomainName.from(host).topPrivateDomain().toString();
    } catch (Exception exception) {
      LOGGER.debug("Error while extracting the primary domain from the host {} ", host, exception);
      return host;
    }
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
    Optional<String> httpUrlForOTelFormat = Optional.empty();
    if (attributeValueMap.containsKey(HTTP_URL.getValue())
        && isAbsoluteUrl(attributeValueMap.get(HTTP_URL.getValue()).getValue())) {
      httpUrlForOTelFormat = Optional.of(attributeValueMap.get(HTTP_URL.getValue()).getValue());
    } else if (attributeValueMap.containsKey(HTTP_SCHEME.getValue())
        && attributeValueMap.containsKey(HTTP_HOST.getValue())
        && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      String url =
          buildUrl(
              getHttpSchemeFromRawAttributes(attributeValueMap).get(),
              attributeValueMap.get(HTTP_HOST.getValue()).getValue(),
              attributeValueMap.get(HTTP_TARGET.getValue()).getValue());
      httpUrlForOTelFormat = Optional.of(url);
    } else if (SpanSemanticConventionUtils.isClientSpanForOtelFormat(attributeValueMap)
        || SpanSemanticConventionUtils.isClientSpanForOCFormat(attributeValueMap)) {
      httpUrlForOTelFormat = getHttpUrlForOtelFormatClientSpan(attributeValueMap);
    } else if (SpanSemanticConventionUtils.isServerSpanForOtelFormat(attributeValueMap)
        || SpanSemanticConventionUtils.isServerSpanForOCFormat(attributeValueMap)) {
      httpUrlForOTelFormat = getHttpUrlForOtelFormatServerSpan(attributeValueMap);
    }

    if (httpUrlForOTelFormat.isEmpty() && attributeValueMap.containsKey(HTTP_URL.getValue())) {
      return Optional.of(attributeValueMap.get(HTTP_URL.getValue()).getValue());
    }
    return httpUrlForOTelFormat.flatMap(HttpSemanticConventionUtils::getAbsoluteUrl);
  }

  private static Optional<String> getHttpUrlForOtelFormatClientSpan(
      Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(HTTP_SCHEME.getValue())
        && attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_NAME.getValue())
        && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      final String httpScheme = getHttpSchemeFromRawAttributes(attributeValueMap).get();
      final String httpTarget = attributeValueMap.get(HTTP_TARGET.getValue()).getValue();
      final String netPeerName =
          attributeValueMap.get(OTelSpanSemanticConventions.NET_PEER_NAME.getValue()).getValue();

      String url;
      if (attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())) {
        url =
            buildUrl(
                httpScheme,
                netPeerName,
                attributeValueMap
                    .get(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())
                    .getValue(),
                httpTarget);
      } else {
        url = buildUrl(httpScheme, netPeerName, httpTarget);
      }
      return Optional.of(url);
    } else if (attributeValueMap.containsKey(HTTP_SCHEME.getValue())
        && (attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_SOCK_PEER_ADDR.getValue())
            || attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_IP.getValue()))
        && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      final String httpScheme = getHttpSchemeFromRawAttributes(attributeValueMap).get();
      final String httpTarget = attributeValueMap.get(HTTP_TARGET.getValue()).getValue();
      final String netPeerIp =
          attributeValueMap
              .getOrDefault(
                  OTelSpanSemanticConventions.NET_SOCK_PEER_ADDR.getValue(),
                  attributeValueMap.get(OTelSpanSemanticConventions.NET_PEER_IP.getValue()))
              .getValue();
      String url;
      if (attributeValueMap.containsKey(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())) {
        url =
            buildUrl(
                httpScheme,
                netPeerIp,
                attributeValueMap
                    .get(OTelSpanSemanticConventions.NET_PEER_PORT.getValue())
                    .getValue(),
                httpTarget);
      } else {
        url = buildUrl(httpScheme, netPeerIp, httpTarget);
      }
      return Optional.of(url);
    }
    return Optional.empty();
  }

  private static Optional<String> getHttpUrlForOtelFormatServerSpan(
      Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(HTTP_SCHEME.getValue())
        && attributeValueMap.containsKey(HTTP_SERVER_NAME.getValue())
        && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      final String httpScheme = getHttpSchemeFromRawAttributes(attributeValueMap).get();
      final String serverName = attributeValueMap.get(HTTP_SERVER_NAME.getValue()).getValue();
      final String httpTarget = attributeValueMap.get(HTTP_TARGET.getValue()).getValue();
      String url;
      if (attributeValueMap.containsKey(HTTP_NET_HOST_PORT.getValue())) {
        url =
            buildUrl(
                httpScheme,
                serverName,
                attributeValueMap.get(HTTP_NET_HOST_PORT.getValue()).getValue(),
                httpTarget);
      } else {
        url = buildUrl(httpScheme, serverName, httpTarget);
      }
      return Optional.of(url);
    } else if (attributeValueMap.containsKey(HTTP_SCHEME.getValue())
        && attributeValueMap.containsKey(HTTP_NET_HOST_NAME.getValue())
        && attributeValueMap.containsKey(HTTP_TARGET.getValue())) {
      final String httpScheme = getHttpSchemeFromRawAttributes(attributeValueMap).get();
      final String netHostName = attributeValueMap.get(HTTP_NET_HOST_NAME.getValue()).getValue();
      final String httpTarget = attributeValueMap.get(HTTP_TARGET.getValue()).getValue();

      String url;
      if (attributeValueMap.containsKey(HTTP_NET_HOST_PORT.getValue())) {
        url =
            buildUrl(
                httpScheme,
                netHostName,
                attributeValueMap.get(HTTP_NET_HOST_PORT.getValue()).getValue(),
                httpTarget);
      } else {
        url = buildUrl(httpScheme, netHostName, httpTarget);
      }
      return Optional.of(url);
    }
    return Optional.empty();
  }

  private static String buildUrl(
      final String scheme, final String host, final String port, final String path) {
    return String.format("%s://%s:%s%s", scheme, host, port, path);
  }

  private static String buildUrl(final String scheme, final String host, final String path) {
    return String.format("%s://%s%s", scheme, host, path);
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

  // https://www.baeldung.com/java-validate-url#validate-url-using-jdk
  public static boolean isAbsoluteUrl(String urlStr) {
    try {
      new URL(urlStr).toURI();
      return true;
    } catch (MalformedURLException | URISyntaxException e) {
      // ignore
    }
    return false;
  }

  /**
   * accepts any absolute or relative URL. e.g. absolute URL:
   * http://hypertrace.org/customer?customer=392 relative URL: /customer?customer=392
   */
  public static boolean isValidUrl(String url) {
    try {
      getNormalizedUrl(url);
    } catch (MalformedURLException | URISyntaxException e) {
      return false;
    }
    return true;
  }

  public static URL getNormalizedUrl(String url) throws MalformedURLException, URISyntaxException {
    String sanitizedUrl = StringUtils.stripStart(url, SLASH);
    URL absoluteUrl = new URL(new URL(RELATIVE_URL_CONTEXT), sanitizedUrl);
    return absoluteUrl.toURI().normalize().toURL();
  }

  public static Optional<String> getAbsoluteUrl(String url) {
    if (!isAbsoluteUrl(url)) {
      return Optional.empty();
    }

    try {
      return Optional.of(getNormalizedUrl(url).toString());
    } catch (MalformedURLException | URISyntaxException e) {
      return Optional.empty();
    }
  }

  public static Optional<String> getHttpUserAgent(Event event) {

    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String userAgent : USER_AGENT_ATTRIBUTES) {
      if (attributeValueMap.get(userAgent) != null
          && !StringUtils.isEmpty(attributeValueMap.get(userAgent).getValue())) {
        return Optional.of(attributeValueMap.get(userAgent).getValue());
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
    Optional<String> url = getHttpUrlFromRawAttributes(event);
    if (url.isPresent() && isAbsoluteUrl(url.get())) {
      try {
        return Optional.of(getNormalizedUrl(url.get()).getAuthority());
      } catch (MalformedURLException | URISyntaxException e) {
        LOGGER.warn(
            "On extracting httpHost, received an invalid URL: {}, {}", url.get(), e.getMessage());
      }
    }
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(event, HOST_ATTRIBUTES));
  }

  public static Optional<String> getHttpPath(Event event) {
    Optional<String> path = getHttpPathFromRawAttributes(event);
    Optional<String> url = getHttpUrlFromRawAttributes(event);
    if (url.isPresent() && path.isEmpty()) {
      try {
        String pathVal = getNormalizedUrl(url.get()).getPath();
        if (StringUtils.isBlank(pathVal)) {
          pathVal = SLASH;
        }
        return Optional.of(removeTrailingSlash(pathVal));
      } catch (MalformedURLException | URISyntaxException e) {
        LOGGER.warn(
            "On extracting httpPath, received an invalid URL: {}, {}", url.get(), e.getMessage());
      }
    }
    return path;
  }

  private static Optional<String> getHttpPathFromRawAttributes(Event event) {

    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for (String path : URL_PATH_ATTRIBUTES) {
      if (attributeValueMap.get(path) != null) {
        String s = attributeValueMap.get(path).getValue();
        if (StringUtils.isNotBlank(s) && s.startsWith(SLASH)) {
          return getPathFromUrlObject(s).map(HttpSemanticConventionUtils::removeTrailingSlash);
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

    Optional<String> scheme = Optional.empty();
    // 1. check for http.url attribute and extract scheme from it if it is a full URL
    Optional<String> url = getHttpUrlFromRawAttributes(event);
    if (url.isPresent() && isAbsoluteUrl(url.get())) {
      try {
        scheme = Optional.of(getNormalizedUrl(url.get()).getProtocol());
      } catch (MalformedURLException | URISyntaxException e) {
        LOGGER.warn(
            "On extracting httpScheme, received an invalid URL: {}, {}", url.get(), e.getMessage());
      }
    }

    // 2. if scheme was extracted from step 1) then check if it is 'https' and return it.
    Optional<String> schemeFromOtherRawAttributes;
    if (scheme.isPresent()) {
      if (scheme.get().equalsIgnoreCase("https")) {
        return scheme;
      }
      // 3. else check if there are other attributes that like 'origin', 'x-forwarded-proto' etc
      // that have the actual scheme
      //    this is for scenarios where a LB fronts the service and terminates SSL.
      //    If this scheme is 'https' then return this instead
      else {
        schemeFromOtherRawAttributes = getHttpSchemeFromRawAttributes(event);
        if (schemeFromOtherRawAttributes.isPresent()
            && schemeFromOtherRawAttributes.get().equalsIgnoreCase("https")) {
          return schemeFromOtherRawAttributes;
        }
      }
      // 4. Just return the scheme which should be 'http'
      return scheme;
    }
    // 5. If scheme from URL couldn't be extracted then try the raw attributes
    return getHttpSchemeFromRawAttributes(event);
  }

  private static Optional<String> getHttpSchemeFromRawAttributes(Event event) {

    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    return getHttpSchemeFromRawAttributes(event.getAttributes().getAttributeMap());
  }

  private static Optional<String> getHttpSchemeFromRawAttributes(
      Map<String, AttributeValue> attributeValueMap) {
    // dealing with the Forwarded header separately as it may have
    // more info than just the protocol
    AttributeValue httpRequestForwardedAttributeValue =
        attributeValueMap.get(HTTP_REQUEST_FORWARDED_ATTRIBUTE);
    if (httpRequestForwardedAttributeValue != null
        && !StringUtils.isEmpty(httpRequestForwardedAttributeValue.getValue())) {
      String schemeValue = httpRequestForwardedAttributeValue.getValue();
      Optional<String> optionalExtractedProtoValue = getProtocolFromForwarded(schemeValue);
      if (optionalExtractedProtoValue.isPresent()) {
        return optionalExtractedProtoValue;
      }
    }

    AttributeValue httpRequestOriginValue = attributeValueMap.get(HTTP_REQUEST_ORIGIN);
    if (httpRequestOriginValue != null && !StringUtils.isEmpty(httpRequestOriginValue.getValue())) {
      String origin = httpRequestOriginValue.getValue();
      /** Syntax: Origin: null Origin: <scheme>://<hostname> Origin: <scheme>://<hostname>:<port> */
      if (StringUtils.isNotEmpty(origin)) {
        try {
          String scheme = new URI(origin).getScheme();
          // handle the case where the value of origin is the "null" string
          if (StringUtils.isNotEmpty(scheme)) {
            return Optional.of(scheme);
          }
        } catch (Exception e) {
          LOGGER.warn(
              "On extracting scheme, received an invalid origin header: {}, {}",
              origin,
              e.getMessage());
        }
      }
    }

    for (String scheme : SCHEME_ATTRIBUTES) {
      if (attributeValueMap.get(scheme) != null
          && !StringUtils.isEmpty(attributeValueMap.get(scheme).getValue())) {
        return Optional.of(attributeValueMap.get(scheme).getValue());
      }
    }
    return Optional.empty();
  }

  private static Optional<String> getProtocolFromForwarded(String value) {
    String[] values = value.split(";");
    for (String subValue : values) {
      String[] protoSplits = subValue.trim().split("=");
      if (protoSplits.length == 2 && protoSplits[0].trim().equals("proto")) {
        return Optional.of(protoSplits[1].trim());
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpUrl(Event event) {
    Optional<String> url = getHttpUrlFromRawAttributes(event);
    return url.flatMap(HttpSemanticConventionUtils::getAbsoluteUrl);
  }

  //  input url to populateurlparts
  private static Optional<String> getHttpUrlFromRawAttributes(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    String httpUrlFromRawAttributes = null;

    for (String url : FULL_URL_ATTRIBUTES) {
      if (attributeValueMap.get(url) != null) {
        String urlVal = attributeValueMap.get(url).getValue();
        if (!StringUtils.isBlank(urlVal) && isValidUrl(urlVal)) {
          httpUrlFromRawAttributes = urlVal;
          break;
        }
      }
    }

    if (httpUrlFromRawAttributes != null && isAbsoluteUrl(httpUrlFromRawAttributes)) {
      return Optional.of(httpUrlFromRawAttributes)
          .flatMap(HttpSemanticConventionUtils::getAbsoluteUrl);
    }

    // check for OTel format
    Optional<String> httpUrlForOTelFormat = getHttpUrlForOTelFormat(attributeValueMap);
    if (httpUrlForOTelFormat.isPresent()) {
      return httpUrlForOTelFormat;
    }

    // The URL is not absolute URL
    return Optional.ofNullable(httpUrlFromRawAttributes);
  }

  public static Optional<String> getHttpQueryString(Event event) {

    Optional<String> queryString =
        Optional.ofNullable(
            SpanAttributeUtils.getFirstAvailableStringAttribute(event, QUERY_STRING_ATTRIBUTES));

    Optional<String> url = getHttpUrlFromRawAttributes(event);
    if (url.isPresent() && queryString.isEmpty()) {
      try {
        return Optional.ofNullable(getNormalizedUrl(url.get()).getQuery());
      } catch (MalformedURLException | URISyntaxException e) {
        LOGGER.warn(
            "On extracting httpQueryString, received an invalid URL: {}, {}",
            url.get(),
            e.getMessage());
      }
    }
    return queryString;
  }

  public static Optional<Integer> getHttpRequestSize(Event event) {
    String httpRequestSize =
        SpanAttributeUtils.getFirstAvailableStringAttribute(event, REQUEST_SIZE_ATTRIBUTES);

    Optional<String> requestSize = Optional.ofNullable(httpRequestSize);
    if (!requestSize.isEmpty()) {
      return requestSize.map(Integer::parseInt);
    }

    if (SpanAttributeUtils.getBooleanAttribute(event, HTTP_REQUEST_BODY_TRUNCATED_ATTR)) {
      return Optional.empty();
    }

    String requestBody = SpanAttributeUtils.getStringAttribute(event, HTTP_REQUEST_BODY);
    return Optional.ofNullable(requestBody).map(String::length);
  }

  public static Optional<Integer> getHttpResponseSize(Event event) {
    String httpResponseSize =
        SpanAttributeUtils.getFirstAvailableStringAttribute(event, RESPONSE_SIZE_ATTRIBUTES);

    Optional<String> responseSize = Optional.ofNullable(httpResponseSize);
    if (!responseSize.isEmpty()) {
      return responseSize.map(Integer::parseInt);
    }

    if (SpanAttributeUtils.getBooleanAttribute(event, HTTP_RESPONSE_BODY_TRUNCATED_ATTR)) {
      return Optional.empty();
    }

    String responseBody = SpanAttributeUtils.getStringAttribute(event, HTTP_RESPONSE_BODY);
    return Optional.ofNullable(responseBody).map(String::length);
  }

  public static int getHttpResponseStatusCode(Event event) {

    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return 0;
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();

    for (String responseStatusCodeKey : STATUS_CODE_ATTRIBUTES) {
      if (attributeValueMap.get(responseStatusCodeKey) != null) {
        return Integer.parseInt(attributeValueMap.get(responseStatusCodeKey).getValue());
      }
    }

    return 0;
  }

  public static Optional<String> getHttpRequestHeaderPath(Event event) {

    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    if (attributeValueMap.get(RawSpanConstants.getValue(HTTP_REQUEST_HEADER_PATH)) != null) {
      return Optional.of(
          attributeValueMap.get(RawSpanConstants.getValue(HTTP_REQUEST_HEADER_PATH)).getValue());
    }
    return Optional.empty();
  }

  public static Map<String, String> getHttpHeadersExceptCookies(
      Event event, Predicate<Map.Entry<String, AttributeValue>> notCookieFilter, String prefix) {
    if (SpanSemanticConventionUtils.isEmptyAttributesMap(event)) {
      return Collections.emptyMap();
    }

    Map<String, AttributeValue> attributes = event.getAttributes().getAttributeMap();

    return attributes.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(prefix))
        .filter(notCookieFilter)
        .filter(entry -> SpanSemanticConventionUtils.isValueNotNull(entry.getValue()))
        .collect(
            Collectors.toUnmodifiableMap(
                entry -> entry.getKey().substring(prefix.length()),
                entry -> entry.getValue().getValue()));
  }

  public static Map<String, String> getHttpRequestCookies(Event event) {
    String requestCookie = getStringAttribute(event, REQUEST_COOKIE_HEADER_KEY);
    if (requestCookie == null || requestCookie.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, String> cookies = new HashMap<>();

    List<String> cookiePairs = SEMICOLON_SPLITTER.splitToList(requestCookie);
    for (String cookiePair : cookiePairs) {
      List<String> cookieKeyValue = COOKIE_KEY_VALUE_SPLITTER.splitToList(cookiePair);
      if (cookieKeyValue.size() != 2) {
        LOGGER.debug(
            "Invalid cookie pair {} for tenant {} and event {}",
            cookiePair,
            event.getCustomerId(),
            HexUtils.getHex(event.getEventId()));
        continue;
      }

      cookies.put(cookieKeyValue.get(0), cookieKeyValue.get(1));
    }

    return Collections.unmodifiableMap(cookies);
  }

  public static Map<String, String> getHttpResponseCookies(Event event) {
    if (SpanSemanticConventionUtils.isEmptyAttributesMap(event)) {
      return Collections.emptyMap();
    }

    List<HttpCookie> cookies = new ArrayList<>();

    Map<String, AttributeValue> attributes = event.getAttributes().getAttributeMap();
    for (Map.Entry<String, AttributeValue> entry : attributes.entrySet()) {
      String attributeKey = entry.getKey();

      if (!isHttpResponseCookie(attributeKey)) {
        continue;
      }

      String attributeValue = entry.getValue().getValue();
      try {
        cookies.addAll(HttpCookie.parse(attributeValue));
      } catch (Exception e) {
        LOGGER.debug(
            "Unable to parse cookie for header key {} and value {} for tenant {} and event {}",
            attributeKey,
            attributeValue,
            event.getCustomerId(),
            HexUtils.getHex(event.getEventId()),
            e);
      }
    }

    return cookies.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                HttpCookie::getName, HttpCookie::getValue, (cookie1, cookie2) -> cookie1));
  }

  public static Optional<String> getHttpXForwardedFor(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, List.of(RawSpanConstants.getValue(HTTP_REQUEST_X_FORWARDED_FOR_HEADER))));
  }

  public static Optional<String> getHttpRequestContentType(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, List.of(RawSpanConstants.getValue(HTTP_REQUEST_CONTENT_TYPE))));
  }

  public static Optional<String> getHttpResponseContentType(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, List.of(RawSpanConstants.getValue(HTTP_RESPONSE_CONTENT_TYPE))));
  }

  public static boolean isHttpRequestCookie(String requestHeaderAttributeKey) {
    return requestHeaderAttributeKey.equals(REQUEST_COOKIE_HEADER_KEY);
  }

  public static boolean isHttpResponseCookie(String responseHeaderAttributeKey) {
    return responseHeaderAttributeKey.startsWith(RESPONSE_COOKIE_HEADER_PREFIX);
  }

  static Optional<String> getPathFromUrlObject(String urlPath) {
    try {
      URL url = getNormalizedUrl(urlPath);
      return Optional.of(url.getPath());
    } catch (MalformedURLException | URISyntaxException e) {
      LOGGER.warn(
          "On extracting httpResponseStatusCode, received invalid URL path : {}, {}",
          urlPath,
          e.getMessage());
    }
    return Optional.empty();
  }

  private static String removeTrailingSlash(String s) {
    // Ends with "/" and it's not home page path
    return s.endsWith(SLASH) && s.length() > 1 ? s.substring(0, s.length() - 1) : s;
  }

  @Nullable
  private static String getStringAttribute(Event event, String attributeKey) {
    AttributeValue value = SpanAttributeUtils.getAttributeValue(event, attributeKey);
    return value == null ? null : value.getValue();
  }
}
