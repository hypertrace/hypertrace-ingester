package org.hypertrace.telemetry.attribute.utils.http;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ApiStatus;

/**
 * Maps the HTTP Status Code to Message, and vice versa.
 * <p>
 * Use these standard: https://tools.ietf.org/html/rfc7231, https://tools.ietf.org/html/rfc7233, and
 * https://tools.ietf.org/html/rfc7235
 * Also on the Mozilla website: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status
 */
public class HttpCodeMapper {
  private static final String HTTP_SUCCESS_CODE_PREFIX = "2";
  private static final String REDIRECTION_CODE_PREFIX = "3";

  private static Map<String, String> codeToMessage = new ImmutableMap.Builder<String, String>()
      // Successful 2xx
      .put("200", "OK")
      .put("201", "Created")
      .put("202", "Accepted")
      .put("203", "Non-Authoritative Information")
      .put("204", "No Content")
      .put("205", "Reset Content")
      .put("206", "Partial Content")
      // Redirection 3xx
      .put("300", "Multiple Choices")
      .put("301", "Moved Permanently")
      .put("302", "Found")
      .put("303", "See Other")
      .put("304", "Not Modified")
      .put("305", "Use Proxy")
      .put("306", "(Unused)")
      .put("307", "Temporary Redirect")
      .put("308", "Permanent Redirect")
      // Client Error 4xx
      .put("400", "Bad Request")
      .put("401", "Unauthorized")
      .put("402", "Payment Required")
      .put("403", "Forbidden")
      .put("404", "Not Found")
      .put("405", "Method Not Allowed")
      .put("406", "Not Acceptable")
      .put("407", "Proxy Authentication Required")
      .put("408", "Request Timeout")
      .put("409", "Conflict")
      .put("410", "Gone")
      .put("411", "Length Required")
      .put("412", "Precondition Failed")
      .put("413", "Payload Too Large")
      .put("414", "URI Too Long")
      .put("415", "Unsupported Media Type")
      .put("416", "Range Not Satisfiable")
      .put("417", "Expectation Failed")
      .put("418", "I'm a teapot")
      .put("425", "Too Early")
      .put("426", "Upgrade Required")
      .put("428", "Precondition Required")
      .put("429", "Too Many Requests")
      .put("431", "Request Header Fields Too Large")
      .put("451", "Unavailable For Legal Reasons")
      // Server Error 5xx
      .put("500", "Internal Server Error")
      .put("501", "Not Implemented")
      .put("502", "Bad Gateway")
      .put("503", "Service Unavailable")
      .put("504", "Gateway Timeout")
      .put("505", "505 HTTP Version Not Supported")
      .put("506", "Variant Also Negotiates")
      .put("510", "Not Extended")
      .put("511", "Network Authentication Required")
      .build();

  public static String getMessage(String code) {
    if (code == null) {
      return null;
    }
    return codeToMessage.get(code);
  }

  public static String getState(String code) {
    if (code == null) {
      return null;
    }
    if (code.startsWith(HTTP_SUCCESS_CODE_PREFIX) || code.startsWith(REDIRECTION_CODE_PREFIX)) {
      return EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_SUCCESS);
    }
    return EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_FAIL);
  }
}
