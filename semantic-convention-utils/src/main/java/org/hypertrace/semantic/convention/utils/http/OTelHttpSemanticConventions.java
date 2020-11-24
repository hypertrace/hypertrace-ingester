package org.hypertrace.semantic.convention.utils.http;

/**
 * OTel specific attributes for Http
 */
public enum OTelHttpSemanticConventions {
  HTTP_METHOD("http.method"),
  HTTP_STATUS_CODE("http.status_code");

  private final String value;

  OTelHttpSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
