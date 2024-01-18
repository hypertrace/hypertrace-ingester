package org.hypertrace.traceenricher.enrichment;

import static org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry.registerCounter;
import static org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants.DROP_TRACE_ATTRIBUTE;

import com.google.common.util.concurrent.ExecutionError;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Edge;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.util.AvroToJsonLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichmentProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(EnrichmentProcessor.class);
  private static final String ENRICHMENT_ARRIVAL_TIME = "enrichment.arrival.time";
  private static final Timer enrichmentArrivalTimer =
      PlatformMetricsRegistry.registerTimer(DataflowMetricUtils.ARRIVAL_LAG, new HashMap<>());

  // Must use linked hashmap
  private final Map<String, Enricher> enrichers = new LinkedHashMap<>();

  private static final String ENRICHED_TRACES_COUNTER = "hypertrace.enriched.traces";
  private static final ConcurrentMap<String, Counter> traceCounters = new ConcurrentHashMap<>();

  private static final String ENRICHED_TRACES_TIMER = "hypertrace.trace.enrichment.latency";
  private static final ConcurrentMap<String, Timer> traceTimers = new ConcurrentHashMap<>();

  private static final String TRACE_ENRICHMENT_ERRORS_COUNTER =
      "hypertrace.trace.enrichment.errors";
  private static final ConcurrentMap<String, Counter> traceErrorsCounters =
      new ConcurrentHashMap<>();

  public EnrichmentProcessor(List<EnricherInfo> enricherInfoList, ClientRegistry clientRegistry) {
    for (EnricherInfo enricherInfo : enricherInfoList) {
      try {
        if (enrichers.containsKey(enricherInfo.getName())) {
          LOG.error("Duplicate enricher found. enricher name: {}", enricherInfo.getName());
          throw new RuntimeException(
              "Configuration error. Duplicate enricher found. enricher name: {}"
                  + enricherInfo.getName());
        }
        Enricher enricher = enricherInfo.getClazz().getDeclaredConstructor().newInstance();
        enricher.init(enricherInfo.getEnricherConfig(), clientRegistry);
        LOG.info("Initialized the enricher: {}", enricherInfo.getClazz().getCanonicalName());
        enrichers.put(enricherInfo.getName(), enricher);
      } catch (Exception e) {
        LOG.error("Exception initializing enricher:{}", enricherInfo, e);
      }
    }
  }

  /**
   * Enriches the Trace by Invoking various Enrichers registered in returns a boolean - to be
   * forward trace downstream or not
   */
  public boolean process(StructuredTrace trace) {
    DataflowMetricUtils.reportArrivalLagAndInsertTimestamp(
        trace, enrichmentArrivalTimer, ENRICHMENT_ARRIVAL_TIME);
    AvroToJsonLogger.log(LOG, "Structured Trace before all the enrichment is: {}", trace);
    for (Entry<String, Enricher> entry : enrichers.entrySet()) {
      Map<String, AttributeValue> attributeMap = trace.getAttributes().getAttributeMap();
      AttributeValue dropTraceAttrValue = attributeMap.get(DROP_TRACE_ATTRIBUTE);
      boolean shouldDropTrace = dropTraceAttrValue != null;
      if (shouldDropTrace) {
        return false;
      }
      String enricherName = entry.getKey();
      Map<String, String> metricTags = Map.of("enricher", enricherName);
      try {
        Instant start = Instant.now();
        applyEnricher(entry.getValue(), trace);
        long timeElapsed = Duration.between(start, Instant.now()).toMillis();

        traceCounters
            .computeIfAbsent(
                enricherName, k -> registerCounter(ENRICHED_TRACES_COUNTER, metricTags))
            .increment();
        traceTimers
            .computeIfAbsent(
                enricherName,
                k -> PlatformMetricsRegistry.registerTimer(ENRICHED_TRACES_TIMER, metricTags))
            .record(timeElapsed, TimeUnit.MILLISECONDS);
      } catch (Exception | ExecutionError throwable) {
        String errorMessage =
            String.format(
                "Could not apply the enricher: %s to the trace with traceId: %s",
                entry.getKey(), HexUtils.getHex(trace.getTraceId()));
        traceErrorsCounters
            .computeIfAbsent(
                enricherName, k -> registerCounter(TRACE_ENRICHMENT_ERRORS_COUNTER, metricTags))
            .increment();
        LOG.error(errorMessage, throwable);
      }
    }
    AvroToJsonLogger.log(LOG, "Structured Trace after all the enrichment is: {}", trace);
    return true;
  }

  private void applyEnricher(Enricher enricher, StructuredTrace trace) {
    // Enrich entities
    List<Entity> entityList = trace.getEntityList();
    LOG.debug("Enriching Entities for {}", enricher.getClass().getName());
    for (Entity entity : entityList) {
      enricher.enrichEntity(trace, entity);
    }
    enricher.onEnrichEntitiesComplete(trace);

    LOG.debug("Enriching Events for {}", enricher.getClass().getName());
    // Enrich Events
    List<Event> eventList = trace.getEventList();
    for (Event event : eventList) {
      enricher.enrichEvent(trace, event);
    }

    // Enrich Edges
    List<Edge> eventEdgeList = trace.getEventEdgeList();
    for (Edge edge : eventEdgeList) {
      enricher.enrichEdge(trace, edge);
    }

    List<Edge> entityEdgeList = trace.getEntityEdgeList();
    for (Edge edge : entityEdgeList) {
      enricher.enrichEdge(trace, edge);
    }

    List<Edge> entityEventEdgeList = trace.getEntityEventEdgeList();
    for (Edge edge : entityEventEdgeList) {
      enricher.enrichEdge(trace, edge);
    }

    LOG.debug(
        "Enriching Trace ID {} for {}",
        HexUtils.getHex(trace.getTraceId()),
        enricher.getClass().getName());

    // Enrich trace attributes/metrics
    enricher.enrichTrace(trace);
  }
}
