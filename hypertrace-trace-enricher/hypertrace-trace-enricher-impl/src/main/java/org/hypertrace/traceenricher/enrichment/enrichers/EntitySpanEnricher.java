package org.hypertrace.traceenricher.enrichment.enrichers;

import com.typesafe.config.Config;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.trace.accessor.entities.TraceEntityAccessor;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntitySpanEnricher extends AbstractTraceEnricher {
  private static final Logger LOG = LoggerFactory.getLogger(EntitySpanEnricher.class);

  private TraceEntityAccessor entityAccessor;

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    try {
      this.entityAccessor.writeAssociatedEntitiesForSpanEventually(trace, event);
    } catch (Exception exception) {
      LOG.error("Failed to enrich entities on span", exception);
    }
  }

  @Override
  public void init(Config enricherConfig, ClientRegistry clientRegistry) {
    this.entityAccessor = clientRegistry.getTraceEntityAccessor();
  }
}
