package org.hypertrace.trace.reader;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

interface ValueSource {
  Optional<LiteralValue> getAttribute(String key, AttributeKind attributeKind);

  Optional<LiteralValue> getMetric(String key, AttributeKind attributeKind);

  Optional<ValueSource> sourceForScope(String scope);

  static ValueSource forSpan(StructuredTrace trace, Event span) {
    return new SpanValueSource(trace, span, DefaultValueCoercer.INSTANCE);
  }

  static ValueSource forTrace(StructuredTrace trace) {
    return new TraceValueSource(trace, DefaultValueCoercer.INSTANCE);
  }

  String TRACE_SCOPE = "TRACE";
}
