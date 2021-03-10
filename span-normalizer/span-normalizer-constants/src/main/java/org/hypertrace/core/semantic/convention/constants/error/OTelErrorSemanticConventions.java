package org.hypertrace.core.semantic.convention.constants.error;

/** OTel attributes for errors */
public enum OTelErrorSemanticConventions {
  EXCEPTION_TYPE("exception.type"),
  EXCEPTION_MESSAGE("exception.message"),
  EXCEPTION_STACKTRACE("exception.stacktrace");

  private final String value;

  OTelErrorSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
