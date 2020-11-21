package org.hypertrace.attributeutils.http;

/**
 * OTel specific attributes for Http
 */
public enum OTelHttpAttributes {
  HTTP_METHOD("http.method"),
  HTTP_STATUS_CODE("http.status_code");

  private final String value;

  OTelHttpAttributes(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
