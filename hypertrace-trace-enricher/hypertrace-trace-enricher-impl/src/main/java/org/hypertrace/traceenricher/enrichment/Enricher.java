package org.hypertrace.traceenricher.enrichment;

import com.typesafe.config.Config;
import org.hypertrace.core.datamodel.Edge;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.data.service.client.EntityDataServiceClientProvider;

public interface Enricher {

  /**
   * Provides the configuration needed for the enricher.
   */
  void init(Config enricherConfig, EntityDataServiceClientProvider provider);

  /**
   * Enrich the attributes/metrics for an Entity
   */
  void enrichEntity(StructuredTrace trace, Entity entity);

  /**
   * Called after all the entities in the trace are enriched
   */
  void onEnrichEntitiesComplete(StructuredTrace trace);

  /**
   * Enrich the attributes/metrics for an Event
   */
  void enrichEvent(StructuredTrace trace, Event event);

  /**
   * Enrich the attributes/metrics for an Edge
   */
  void enrichEdge(StructuredTrace trace, Edge edge);

  /**
   * Enrich Trace level attributes/metrics
   */
  void enrichTrace(StructuredTrace trace);

}
