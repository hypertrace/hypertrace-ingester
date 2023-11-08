package org.hypertrace.trace.reader.attributes;

import static org.hypertrace.trace.reader.attributes.ValueSource.TRACE_SCOPE;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.trace.provider.AttributeProvider;

import java.util.Optional;

class DefaultTraceAttributeReader implements TraceAttributeReader<StructuredTrace, Event> {

  private final AttributeProvider attributeProvider;
  private final ValueResolver valueResolver;

  DefaultTraceAttributeReader(AttributeProvider attributeProvider) {
    this.attributeProvider = attributeProvider;
    this.valueResolver = ValueResolver.build(this.attributeProvider);
  }

  @Override
  public Optional<LiteralValue> getSpanValue(
      StructuredTrace trace, Event span, String attributeScope, String attributeKey) {
    ValueSource valueSource = ValueSourceFactory.forSpan(trace, span);
    return this.getAttribute(valueSource, attributeScope, attributeKey)
        .flatMap(definition -> this.valueResolver.resolve(valueSource, definition));
  }

  @Override
  public Optional<LiteralValue> getTraceValue(StructuredTrace trace, String attributeKey) {
    ValueSource valueSource = ValueSourceFactory.forTrace(trace);
    return this.getAttribute(valueSource, TRACE_SCOPE, attributeKey)
        .flatMap(definition -> this.valueResolver.resolve(valueSource, definition));
  }

  @Override
  public String getTenantId(Event span) {
    return span.getCustomerId();
  }

  private Optional<AttributeMetadata> getAttribute(
      ValueSource valueSource, String attributeScope, String attributeKey) {
    return this.attributeProvider.get(valueSource.tenantId(), attributeScope, attributeKey);
  }
}
