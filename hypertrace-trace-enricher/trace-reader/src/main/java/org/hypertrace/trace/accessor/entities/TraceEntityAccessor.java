package org.hypertrace.trace.accessor.entities;

import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

public interface TraceEntityAccessor {

  void writeAssociatedEntitiesForSpanEventually(StructuredTrace trace, Event span);
}
