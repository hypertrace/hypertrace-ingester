package org.hypertrace.trace.reader.entities;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.hypertrace.entity.data.service.v1.Entity;

public interface TraceEntityReader<T extends GenericRecord, S extends GenericRecord> {

  Maybe<Entity> getAssociatedEntityForSpan(String entityType, T trace, S span);

  Single<Map<String, Entity>> getAssociatedEntitiesForSpan(T trace, S span);
}
