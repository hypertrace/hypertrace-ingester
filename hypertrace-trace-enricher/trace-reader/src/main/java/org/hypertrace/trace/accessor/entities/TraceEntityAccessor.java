package org.hypertrace.trace.accessor.entities;

import java.util.Set;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

public interface TraceEntityAccessor {

  void writeAssociatedEntitiesForSpanEventually(
      StructuredTrace trace, Event span, Set<String> entityTypesToBeExcluded);
}
