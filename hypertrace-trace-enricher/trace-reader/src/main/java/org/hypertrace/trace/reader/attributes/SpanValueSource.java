package org.hypertrace.trace.reader.attributes;

import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition.SourceField;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.Resource;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
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
        .or(() -> this.getResourceAttributeString(key))
        .flatMap(stringValue -> ValueCoercer.toLiteral(stringValue, attributeKind));
  }

  @Override
  public Optional<LiteralValue> getMetric(String key, AttributeKind attributeKind) {
    return this.getMetricDouble(this.span.getMetrics(), key)
        .flatMap(doubleValue -> ValueCoercer.toLiteral(doubleValue, attributeKind));
  }

  @Override
  public Optional<LiteralValue> getSourceField(SourceField field, AttributeKind attributeKind) {
    switch (field) {
      case SOURCE_FIELD_START_TIME:
        return ValueCoercer.toLiteral(this.span.getStartTimeMillis(), attributeKind);
      case SOURCE_FIELD_END_TIME:
        return ValueCoercer.toLiteral(this.span.getEndTimeMillis(), attributeKind);
      case SOURCE_FIELD_UNSET:
      case UNRECOGNIZED:
      default:
        throw new UnsupportedOperationException("Unsupported source field for span value source: " + field.name());
    }
  }

  @Override
  public Optional<ValueSource> sourceForScope(String scope) {
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

  private Optional<String> getResourceAttributeString(String key) {
    if (span.getResourceIndex() < 0 || span.getResourceIndex() >= trace.getResourceList().size()) {
      return Optional.empty();
    }

    return Optional.of(trace.getResourceList().get(span.getResourceIndex()))
        .map(Resource::getAttributes)
        .flatMap(attributes -> this.getAttributeString(attributes, key));
  }
}
