package org.hypertrace.trace.reader.attributes;

import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

class ValueSourceFactory {
  static ValueSource forSpan(StructuredTrace trace, Event span) {
    return new SpanValueSource(trace, span);
  }

  static ValueSource forTrace(StructuredTrace trace) {
    return new TraceValueSource(trace);
  }
}
