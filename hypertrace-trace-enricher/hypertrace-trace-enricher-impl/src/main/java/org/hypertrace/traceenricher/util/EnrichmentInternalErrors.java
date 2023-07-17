package org.hypertrace.traceenricher.util;

public enum EnrichmentInternalErrors {
  INTERNAL_ERROR("enricher.internal.error");

  private final String value;

  EnrichmentInternalErrors(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
