package org.hypertrace.trace.reader.attributes;

import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

class ValueSourceFactory {
  static ValueSource<StructuredTrace, Event> forSpan(StructuredTrace trace, Event span) {
    return new SpanValueSource(trace, span);
  }

  static ValueSource<StructuredTrace, Event> forTrace(StructuredTrace trace) {
    return new TraceValueSource(trace);
  }
}
