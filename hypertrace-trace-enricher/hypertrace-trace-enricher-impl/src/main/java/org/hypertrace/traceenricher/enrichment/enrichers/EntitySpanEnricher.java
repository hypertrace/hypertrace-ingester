package org.hypertrace.traceenricher.enrichment.enrichers;

import com.typesafe.config.Config;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.trace.reader.entities.TraceEntityReader;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntitySpanEnricher extends AbstractTraceEnricher {
  private static final Logger LOG = LoggerFactory.getLogger(EntitySpanEnricher.class);
  private TraceEntityReader<StructuredTrace, Event> entityReader;

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    try {
      this.entityReader.getAssociatedEntitiesForSpan(trace, event).blockingSubscribe();
    } catch (Throwable t) {
      LOG.error("Failed to enrich entities on span", t);
    }
  }

  @Override
  public void init(Config enricherConfig, ClientRegistry clientRegistry) {
    this.entityReader = clientRegistry.getEntityReader();
  }
}
