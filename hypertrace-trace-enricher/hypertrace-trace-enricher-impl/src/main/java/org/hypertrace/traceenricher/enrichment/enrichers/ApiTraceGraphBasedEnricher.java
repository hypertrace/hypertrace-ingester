package org.hypertrace.traceenricher.enrichment.enrichers;

import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;

/**
 * {@link org.hypertrace.traceenricher.trace.util.ApiTraceGraph} is built in this enricher
 * and any enrichment based on it should be performed here
 */
public class ApiTraceGraphBasedEnricher extends AbstractTraceEnricher {

  @Override
  public void enrichTrace(StructuredTrace trace) {
    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace).build();

    trace.getEventList()
        .stream()
        .filter(EnrichedSpanUtils::isEntryApiBoundary)
        .forEach(event -> {
          addEnrichedAttribute(
              event, EnrichedSpanConstants.API_EXIT_CALL_COUNT_ATTRIBUTE, AttributeValueCreator.create(
                  apiTraceGraph.getApiNodeEdgesForEvent(event.getEventId()).size()));
        });
  }
}
