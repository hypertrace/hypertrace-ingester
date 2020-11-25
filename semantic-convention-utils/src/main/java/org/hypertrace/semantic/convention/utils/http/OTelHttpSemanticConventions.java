package org.hypertrace.semantic.convention.utils.http;

/**
 * OTel specific attributes for Http
 */
public enum OTelHttpSemanticConventions {
  HTTP_METHOD("http.method"),
  HTTP_STATUS_CODE("http.status_code"),
  HTTP_TARGET("http.target"),
  HTTP_USER_AGENT("http.user_agent"),
  HTTP_HOST("http.host"),
  HTTP_SERVER_NAME("http.server_name"),
  HTTP_URL("http.url"),
  HTTP_NET_HOST_NAME("net.host.name"),
  HTTP_NET_HOST_PORT("net.host.port"),
  HTTP_SCHEME("http.schema"),
  HTTP_REQUEST_SIZE("http.request_content_length"),
  HTTP_RESPONSE_SIZE("http.response_content_length");

  private final String value;

  OTelHttpSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
