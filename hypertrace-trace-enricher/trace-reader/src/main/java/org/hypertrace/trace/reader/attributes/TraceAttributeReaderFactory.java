package org.hypertrace.trace.reader.attributes;

import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

public interface TraceAttributeReaderFactory {
  static TraceAttributeReader<StructuredTrace, Event> build(
      AttributeServiceCachedClient attributeClient) {
    return new DefaultTraceAttributeReader(attributeClient);
  }
}
