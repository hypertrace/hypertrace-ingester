package org.hypertrace.traceenricher.enrichment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Timer;
import org.hypertrace.core.datamodel.Edge;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.DataflowMetric;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.data.service.client.EntityDataServiceClientProvider;
import org.hypertrace.traceenricher.util.AvroToJsonLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichmentProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(EnrichmentProcessor.class);
  private static final String ENRICHMENT_ARRIVAL_LAG = "enrichment.arrival.lag";
  private static final Timer enrichmentArrivalTimer =
      PlatformMetricsRegistry.registerTimer(ENRICHMENT_ARRIVAL_LAG, new HashMap<>());
  private final List<Enricher> enrichers = new ArrayList<>();
  public EnrichmentProcessor(List<EnricherInfo> enricherInfoList,
                             EntityDataServiceClientProvider provider) {
    for (EnricherInfo enricherInfo : enricherInfoList) {
      try {
        Enricher enricher = enricherInfo.getClazz().getDeclaredConstructor().newInstance();
        enricher.init(enricherInfo.getEnricherConfig(), provider);
        LOG.info("Initialized the enricher: {}", enricherInfo.getClazz().getCanonicalName());
        enrichers.add(enricher);
      } catch (Exception e) {
        LOG.error("Exception initializing enricher:{}", enricherInfo, e);
      }
    }
  }

  /**
   * Enriches the Trace by Invoking various Enrichers registered in
   */
  public void process(StructuredTrace trace) {
    DataflowMetricUtils.reportArrivalLagAndInsertTimestamp(trace, enrichmentArrivalTimer,
        DataflowMetric.ENRICHMENT_ARRIVAL_TIME);
    AvroToJsonLogger.log(LOG, "Structured Trace before all the enrichment is: {}", trace);
    for (Enricher enricher : enrichers) {
      applyEnricher(enricher, trace);
    }
    AvroToJsonLogger.log(LOG, "Structured Trace after all the enrichment is: {}", trace);
  }

  private void applyEnricher(Enricher enricher, StructuredTrace trace) {
    //Enrich entities
    List<Entity> entityList = trace.getEntityList();
    LOG.debug("Enriching Entities for {}", enricher.getClass().getName());
    for (Entity entity : entityList) {
      enricher.enrichEntity(trace, entity);
    }
    enricher.onEnrichEntitiesComplete(trace);

    LOG.debug("Enriching Events for {}", enricher.getClass().getName());
    //Enrich Events
    List<Event> eventList = trace.getEventList();
    for (Event event : eventList) {
      enricher.enrichEvent(trace, event);
    }

    //Enrich Edges
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

    LOG.debug("Enriching Trace ID {} for {}", HexUtils.getHex(trace.getTraceId()),
        enricher.getClass().getName());

    //Enrich trace attributes/metrics
    enricher.enrichTrace(trace);
  }
}
