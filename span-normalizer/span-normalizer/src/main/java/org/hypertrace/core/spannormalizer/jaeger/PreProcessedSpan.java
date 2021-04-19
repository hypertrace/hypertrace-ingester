package org.hypertrace.core.spannormalizer.jaeger;

import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;

public class PreProcessedSpan {

  private final String tenantId;
  private final Span span;

  public PreProcessedSpan(String tenantId, Span span) {
    this.tenantId = tenantId;
    this.span = span;
  }

  public String getTenantId() {
    return tenantId;
  }

  public Span getSpan() {
    return span;
  }
}
