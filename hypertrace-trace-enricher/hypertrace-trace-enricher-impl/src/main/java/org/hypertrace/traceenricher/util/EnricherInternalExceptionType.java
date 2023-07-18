package org.hypertrace.traceenricher.util;

public enum EnricherInternalExceptionType {
  INTERNAL_ERROR("enricher.internal.error");

  private final String value;

  EnricherInternalExceptionType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
