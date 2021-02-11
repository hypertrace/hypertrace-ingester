package org.hypertrace.trace.reader.attributes;

import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.grpcutils.client.rx.GrpcRxExecutionContext;

class SpanValueSource extends AvroBackedValueSource {

  private final StructuredTrace trace;
  private final Event span;

  SpanValueSource(StructuredTrace trace, Event span) {
    this.trace = trace;
    this.span = span;
  }

  @Override
  public Optional<LiteralValue> getAttribute(String key, AttributeKind attributeKind) {
    return this.getAttributeString(this.span.getEnrichedAttributes(), key)
        .or(() -> this.getAttributeString(this.span.getAttributes(), key))
        .flatMap(stringValue -> ValueCoercer.toLiteral(stringValue, attributeKind));
  }

  @Override
  public Optional<LiteralValue> getMetric(String key, AttributeKind attributeKind) {
    return this.getMetricDouble(this.span.getMetrics(), key)
        .flatMap(doubleValue -> ValueCoercer.toLiteral(doubleValue, attributeKind));
  }

  @Override
  public Optional<ValueSource<StructuredTrace, Event>> sourceForScope(String scope) {
    return TRACE_SCOPE.equals(scope)
        ? Optional.of(ValueSourceFactory.forTrace(this.trace))
        : Optional.of(this);
  }

  @Override
  public GrpcRxExecutionContext executionContext() {
    return GrpcRxExecutionContext.forTenantContext(this.span.getCustomerId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SpanValueSource that = (SpanValueSource) o;
    return Objects.equals(trace, that.trace) && Objects.equals(span, that.span);
  }

  @Override
  public int hashCode() {
    return Objects.hash(trace, span);
  }
}
