package org.hypertrace.trace.reader.entities;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.util.Map;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

public interface TraceEntityReader {

  Maybe<Entity> getAssociatedEntityForSpan(String entityType, StructuredTrace trace, Event span);

  Single<Map<String, Entity>> getAssociatedEntitiesForSpan(StructuredTrace trace, Event span);
}
