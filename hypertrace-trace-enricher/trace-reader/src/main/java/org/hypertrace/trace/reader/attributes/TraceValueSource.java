package org.hypertrace.trace.reader.attributes;

import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.grpcutils.client.rx.GrpcRxExecutionContext;

class TraceValueSource extends AvroBackedValueSource {

  private final StructuredTrace trace;

  TraceValueSource(StructuredTrace trace) {
    this.trace = trace;
  }

  @Override
  public Optional<LiteralValue> getAttribute(String key, AttributeKind attributeKind) {
    return this.getAttributeString(this.trace.getAttributes(), key)
        .flatMap(stringValue -> ValueCoercer.toLiteral(stringValue, attributeKind));
  }

  @Override
  public Optional<LiteralValue> getMetric(String key, AttributeKind attributeKind) {
    return this.getMetricDouble(this.trace.getMetrics(), key)
        .flatMap(doubleValue -> ValueCoercer.toLiteral(doubleValue, attributeKind));
  }

  @Override
  public Optional<ValueSource> sourceForScope(String scope) {
    return TRACE_SCOPE.equals(scope) ? Optional.of(this) : Optional.empty();
  }

  @Override
  public GrpcRxExecutionContext executionContext() {
    return GrpcRxExecutionContext.forTenantContext(this.trace.getCustomerId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TraceValueSource that = (TraceValueSource) o;
    return Objects.equals(trace, that.trace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(trace);
  }
}
