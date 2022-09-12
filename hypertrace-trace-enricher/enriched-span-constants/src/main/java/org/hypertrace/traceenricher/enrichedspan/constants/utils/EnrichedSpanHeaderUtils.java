package org.hypertrace.traceenricher.enrichedspan.constants.utils;

import static com.google.common.net.HttpHeaders.COOKIE;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_METADATA;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_METADATA;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_HEADER;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_METADATA;
import static org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils.isRpcSystemGrpc;
import static org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils.isEmptyAttributesMap;
import static org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils.isValueNotNull;

import com.google.common.base.Predicate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.span.constants.RawSpanConstants;

public class EnrichedSpanHeaderUtils {
  private static final String DOT = ".";
  private static final String HTTP_REQUEST_HEADER_PREFIX =
      RawSpanConstants.getValue(HTTP_REQUEST_HEADER) + DOT;
  private static final String GRPC_REQUEST_METADATA_PREFIX =
      RawSpanConstants.getValue(GRPC_REQUEST_METADATA) + DOT;
  private static final String REQUEST_COOKIE_HEADER_KEY =
      HTTP_REQUEST_HEADER_PREFIX + COOKIE.toLowerCase();
  private static final String RPC_REQUEST_METADATA_PREFIX = RPC_REQUEST_METADATA.getValue() + DOT;
  private static final String HTTP_RESPONSE_HEADER_PREFIX =
      RawSpanConstants.getValue(HTTP_RESPONSE_HEADER) + DOT;
  private static final String RESPONSE_COOKIE_HEADER_PREFIX =
      HTTP_RESPONSE_HEADER_PREFIX + SET_COOKIE.toLowerCase();
  private static final String GRPC_RESPONSE_METADATA_PREFIX =
      RawSpanConstants.getValue(GRPC_RESPONSE_METADATA) + DOT;
  private static final String RPC_RESPONSE_METADATA_PREFIX = RPC_RESPONSE_METADATA.getValue() + DOT;

  /**
   * The request headers are populated as key-value pair
   *
   * @return map of headers with identifying prefix removed key -> value
   *     <ul>
   *       <li>header1 -> value1
   *       <li>header2 -> value2
   *       <li>header3 -> value3
   *     </ul>
   */
  public static Map<String, String> getRequestHeadersExceptCookies(Event event) {
    Map<String, String> spanRequestHeaders = new HashMap<>();

    // to get http request headers except cookies we use http.request.header. as prefix
    spanRequestHeaders.putAll(
        getHeadersExceptCookies(
            event,
            entry -> !EnrichedSpanUtils.isHttpRequestCookie(entry.getKey()),
            HTTP_REQUEST_HEADER_PREFIX));

    // to get grpc request headers except cookies we use grpc.request.metadata. as prefix
    spanRequestHeaders.putAll(
        getHeadersExceptCookies(event, entry -> true, GRPC_REQUEST_METADATA_PREFIX));

    // to get rpc request headers except cookies we use rpc.request.metadata. as prefix
    spanRequestHeaders.putAll(
        getHeadersExceptCookies(event, entry -> true, RPC_REQUEST_METADATA_PREFIX));
    return Collections.unmodifiableMap(spanRequestHeaders);
  }

  /**
   * The response headers are populated as key-value pair
   *
   * @return map of headers with identifying prefix removed key -> value
   *     <ul>
   *       <li>header1 -> value1
   *       <li>header2 -> value2
   *       <li>header3 -> value3
   *     </ul>
   */
  public static Map<String, String> getResponseHeadersExceptCookies(Event event) {
    Map<String, String> spanRequestHeaders = new HashMap<>();

    // to get http response headers except cookies we use http.response.header. as prefix
    spanRequestHeaders.putAll(
        getHeadersExceptCookies(
            event,
            entry -> !EnrichedSpanUtils.isHttpResponseCookie(entry.getKey()),
            HTTP_RESPONSE_HEADER_PREFIX));

    // to get grpc response headers except cookies we use grpc.response.metadata. as prefix
    spanRequestHeaders.putAll(
        getHeadersExceptCookies(event, entry -> true, GRPC_RESPONSE_METADATA_PREFIX));

    // to get rpc response headers except cookies we use rpc.response.metadata. as prefix
    spanRequestHeaders.putAll(
        getHeadersExceptCookies(event, entry -> true, RPC_RESPONSE_METADATA_PREFIX));
    return Collections.unmodifiableMap(spanRequestHeaders);
  }

  private static Map<String, String> getHeadersExceptCookies(
      Event event, Predicate<Map.Entry<String, AttributeValue>> notCookieFilter, String prefix) {
    if (isEmptyAttributesMap(event)) {
      return Collections.emptyMap();
    }

    Map<String, AttributeValue> attributes = event.getAttributes().getAttributeMap();

    if (prefix.equalsIgnoreCase(RPC_REQUEST_METADATA_PREFIX) && !isRpcSystemGrpc(attributes)) {
      return Collections.emptyMap();
    }

    return attributes.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(prefix))
        .filter(notCookieFilter)
        .filter(entry -> isValueNotNull(entry.getValue()))
        .collect(
            Collectors.toUnmodifiableMap(
                entry -> entry.getKey().substring(prefix.length()),
                entry -> entry.getValue().getValue()));
  }
}
