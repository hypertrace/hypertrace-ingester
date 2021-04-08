package org.hypertrace.core.semantic.convention.constants.error;

/**
 * OTel attributes for errors
 */
public enum OTelErrorSemanticConventions {
  EXCEPTION_TYPE("exception.type"),
  EXCEPTION_MESSAGE("exception.message"),
  EXCEPTION_STACKTRACE("exception.stacktrace"),
  STATUS_CODE("status.code"),
  STATUS_CODE_UNSET_VALUE("0"),
  STATUS_CODE_OK_VALUE("1"),
  STATUS_CODE_ERROR_VALUE("2");

  private final String value;

  OTelErrorSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
