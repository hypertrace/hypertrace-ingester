package org.hypertrace.traceenricher.enrichment;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
import static org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry.registerCounter;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import io.micrometer.core.instrument.Counter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Edge;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.trace.util.StructuredTraceGraphBuilder;
import org.hypertrace.traceenricher.util.EnricherInternalExceptionType;

public abstract class AbstractTraceEnricher implements Enricher {

  private static final String TRACE_ENRICHMENT_INTERNAL_EXCEPTIONS =
      "hypertrace.trace.enrichment.internal.exceptions";
  private static final ConcurrentMap<String, Counter> exceptionCounters = new ConcurrentHashMap<>();

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

  protected void trackExceptions(StructuredTrace trace, EnricherInternalExceptionType exception) {
    String enricher = this.getClass().getSimpleName();
    String tenantId = trace.getCustomerId();
    String metricKey = String.format("%s/%s/%s", enricher, tenantId, exception.getValue());
    Map<String, String> metricTags =
        Map.of("enricher", enricher, "tenantId", tenantId, "exception", exception.getValue());

    exceptionCounters
        .computeIfAbsent(
            metricKey, k -> registerCounter(TRACE_ENRICHMENT_INTERNAL_EXCEPTIONS, metricTags))
        .increment();
  }
}
