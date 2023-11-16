package org.hypertrace.trace.reader.attributes;

import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.trace.provider.AttributeProvider;

public interface TraceAttributeReaderFactory {
  static TraceAttributeReader<StructuredTrace, Event> build(AttributeProvider attributeProvider) {
    return new DefaultTraceAttributeReader(attributeProvider);
  }
}
