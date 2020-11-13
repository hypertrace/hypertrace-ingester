package org.hypertrace.trace.reader.entities;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.util.Map;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;

public interface TraceEntityReader {

  Maybe<Entity> getAssociatedEntityForSpan(String entityType, StructuredTrace trace, Event span);

  Single<Map<String, Entity>> getAssociatedEntitiesForSpan(StructuredTrace trace, Event span);

  static TraceEntityReader build(
      EntityTypeClient entityTypeClient,
      EntityDataClient entityDataClient,
      CachingAttributeClient attributeClient) {
    return new DefaultTraceEntityReader(
        entityTypeClient,
        entityDataClient,
        attributeClient,
        TraceAttributeReader.build(attributeClient));
  }
}
