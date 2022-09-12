package org.hypertrace.traceenricher.enrichedspan.constants.utils;

import static com.google.common.net.HttpHeaders.COOKIE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER;

import com.google.common.base.Splitter;
import java.net.HttpCookie;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichedSpanCookieUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichedSpanCookieUtils.class);
  private static final String DOT = ".";
  private static final String HTTP_REQUEST_HEADER_PREFIX =
      RawSpanConstants.getValue(HTTP_REQUEST_HEADER) + DOT;
  private static final String REQUEST_COOKIE_HEADER_KEY =
      HTTP_REQUEST_HEADER_PREFIX + COOKIE.toLowerCase();
  private static final Splitter SEMICOLON_SPLITTER =
      Splitter.on(";").trimResults().omitEmptyStrings();
  private static final Splitter COOKIE_KEY_VALUE_SPLITTER =
      Splitter.on("=").limit(2).trimResults().omitEmptyStrings();

  /**
   * The request cookies are populated as `http.request.header.cookie` with value as
   * cookie1=value1;cookie2=value2;cookie3=value3
   *
   * @return map of cookie key -> value
   *     <ul>
   *       <li>cookie1 -> value1
   *       <li>cookie2 -> value2
   *       <li>cookie3 -> value3
   *     </ul>
   */
  public static Map<String, String> getRequestCookies(Event event) {
    String requestCookie = EnrichedSpanUtils.getStringAttribute(event, REQUEST_COOKIE_HEADER_KEY);
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

  /**
   * The response cookies are populated as `http.response.header.set-cookie[0] -> cookie1=value1`,
   * `http.response.header.set-cookie[1] -> cookie2=value2` and so on in span attributes
   *
   * @return map of cookie key -> value *
   *     <ul>
   *       *
   *       <li>cookie1 -> value1
   *       <li>cookie2 -> value2
   *     </ul>
   */
  public static Map<String, String> getResponseCookies(Event event) {
    if (EnrichedSpanUtils.isEmptyAttributesMap(event)) {
      return Collections.emptyMap();
    }

    List<HttpCookie> cookies = new ArrayList<>();

    Map<String, AttributeValue> attributes = event.getAttributes().getAttributeMap();
    for (Map.Entry<String, AttributeValue> entry : attributes.entrySet()) {
      String attributeKey = entry.getKey();

      if (!EnrichedSpanUtils.isHttpResponseCookie(attributeKey)) {
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
}
