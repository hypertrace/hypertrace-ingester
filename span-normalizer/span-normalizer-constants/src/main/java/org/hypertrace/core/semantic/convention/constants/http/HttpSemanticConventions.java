package org.hypertrace.core.semantic.convention.constants.http;

/** Request forwarded specific attributes for Http */
public enum HttpSemanticConventions {
  HTTP_REQUEST_X_FORWARDED_PROTO("http.request.header.x-forwarded-proto"),
  HTTP_REQUEST_FORWARDED("http.request.header.forwarded");

  private final String value;

  HttpSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
