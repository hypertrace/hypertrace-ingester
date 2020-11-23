package org.hypertrace.telemetry.attribute.utils.error;

/**
 * OTel attributes for errors
 */
public enum OTelErrorAttributes {
  EXCEPTION_TYPE("exception.type"),
  EXCEPTION_MESSAGE("exception.message"),
  EXCEPTION_STACKTRACE("exception.stacktrace");

  private final String value;

  OTelErrorAttributes(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
