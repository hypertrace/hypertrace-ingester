package org.hypertrace.core.spannormalizer.jaeger;

import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import org.hypertrace.core.datamodel.Event;

public class PreProcessedSpan {

  private final String tenantId;
  private final Span span;
  private final Event event;

  public PreProcessedSpan(String tenantId, Span span, Event event) {
    this.tenantId = tenantId;
    this.span = span;
    this.event = event;
  }

  public String getTenantId() {
    return tenantId;
  }

  public Span getSpan() {
    return span;
  }

  public Event getEvent() {
    return event;
  }
}
