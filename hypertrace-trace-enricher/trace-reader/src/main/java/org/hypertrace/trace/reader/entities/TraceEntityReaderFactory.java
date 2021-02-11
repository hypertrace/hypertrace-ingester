package org.hypertrace.trace.reader.entities;

import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.trace.reader.attributes.TraceAttributeReaderFactory;

public class TraceEntityReaderFactory {
  public static TraceEntityReader<StructuredTrace, Event> build(TraceEntityClientContext clientContext) {
    return new DefaultTraceEntityReader<>(
        clientContext.getEntityTypeClient(),
        clientContext.getEntityDataClient(),
        clientContext.getAttributeClient(),
        TraceAttributeReaderFactory.build(clientContext.getAttributeClient()));
  }
}
