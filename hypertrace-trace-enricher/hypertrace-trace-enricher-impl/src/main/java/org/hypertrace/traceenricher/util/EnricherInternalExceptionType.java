package org.hypertrace.traceenricher.util;

public enum EnricherInternalExceptionType {
  PROCESS_EXCEPTION("processing.exception");

  private final String value;

  EnricherInternalExceptionType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
