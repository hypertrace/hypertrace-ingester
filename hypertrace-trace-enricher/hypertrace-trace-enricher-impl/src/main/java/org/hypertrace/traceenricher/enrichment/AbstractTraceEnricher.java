package org.hypertrace.traceenricher.enrichment;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import io.micrometer.core.instrument.Tag;
import java.util.*;
import javax.annotation.Nullable;
import org.hypertrace.core.datamodel.*;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.trace.util.StructuredTraceGraphBuilder;
import org.hypertrace.traceenricher.util.EnrichmentInternalErrors;

public abstract class AbstractTraceEnricher implements Enricher {

  @Override
  public void init(Config enricherConfig, ClientRegistry clientRegistry) {}

  @Override
  public void enrichEdge(StructuredTrace trace, Edge edge) {}

  @Override
  public void enrichEntity(StructuredTrace trace, Entity entity) {}

  @Override
  public void onEnrichEntitiesComplete(StructuredTrace structuredTrace) {}

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {}

  @Override
  public void enrichTrace(StructuredTrace trace) {}

  protected void onInternalException(StructuredTrace trace, EnrichmentInternalErrors errors) {
    PlatformMetricsRegistry.getMeterRegistry()
        .counter(
            "enrichment.internal.errors",
            List.of(
                Tag.of("name", this.getClass().getCanonicalName()),
                Tag.of("tenantId", trace.getCustomerId()),
                Tag.of("error", errors.getValue())));
  }

  /** Wrapper to the structure graph factory for testing */
  public StructuredTraceGraph buildGraph(StructuredTrace trace) {
    return StructuredTraceGraphBuilder.buildGraph(trace);
  }

  @Nullable
  protected Event getEarliestEvent(StructuredTrace trace) {
    long earliestEventTime = Long.MAX_VALUE;
    Event earliestEvent = null;
    List<Event> events = trace.getEventList();
    for (Event event : events) {
      long eventStartTime = event.getStartTimeMillis();
      if (eventStartTime < earliestEventTime) {
        earliestEventTime = eventStartTime;
        earliestEvent = event;
      }
    }

    return earliestEvent;
  }

  protected void addEnrichedAttribute(Event event, String key, AttributeValue value) {
    Attributes enrichedAttributes = event.getEnrichedAttributes();
    if (enrichedAttributes == null) {
      enrichedAttributes = fastNewBuilder(Attributes.Builder.class).build();
      event.setEnrichedAttributes(enrichedAttributes);
    }

    enrichedAttributes.getAttributeMap().put(key, value);
  }

  protected void addEnrichedAttributes(
      Event event, Map<String, AttributeValue> attributesToEnrich) {
    attributesToEnrich.forEach((key, value) -> addEnrichedAttribute(event, key, value));
  }

  protected void addEnrichedAttributeIfNotNull(Event event, String key, String value) {
    if (value != null) {
      AttributeValue attributeValue =
          fastNewBuilder(AttributeValue.Builder.class).setValue(value).build();
      addEnrichedAttribute(event, key, attributeValue);
    }
  }

  protected void addEntity(StructuredTrace trace, Event event, Entity entity) {
    // Add this to a list and don't use immutable list since the later stages might be
    // adding more entities.
    event.setEntityIdList(Lists.newArrayList(entity.getEntityId()));

    addEntity(trace, entity);
  }

  protected void addEntity(StructuredTrace trace, Entity entity) {
    Set<String> entityIds = new HashSet<>();
    Objects.requireNonNull(trace.getEntityList()).forEach(e -> entityIds.add(e.getEntityId()));
    if (!entityIds.contains(entity.getEntityId())) {
      trace.getEntityList().add(entity);
    }
  }
}
