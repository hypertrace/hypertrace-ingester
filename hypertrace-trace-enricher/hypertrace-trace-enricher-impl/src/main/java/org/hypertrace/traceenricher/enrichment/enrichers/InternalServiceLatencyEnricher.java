package org.hypertrace.traceenricher.enrichment.enrichers;

import java.util.List;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.util.Constants;

public class InternalServiceLatencyEnricher extends AbstractTraceEnricher {

  private static final String API_BOUNDARY_TYPE_ATTR_NAME =
      EnrichedSpanConstants.getValue(Api.API_BOUNDARY_TYPE);
  private static final String EXIT_BOUNDARY_TYPE =
      EnrichedSpanConstants.getValue(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_EXIT);
  private static final String ENTRY_BOUNDARY_TYPE =
      EnrichedSpanConstants.getValue(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY);

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    var apiBoundaryType = event.getEnrichedAttributes().getAttributeMap()
        .get(API_BOUNDARY_TYPE_ATTR_NAME).getValue();
    if (apiBoundaryType.equals(ENTRY_BOUNDARY_TYPE)) {
      var parentDuration = getEventDuration(event);
      StructuredTraceGraph graph = buildGraph(trace);
      List<Event> children = graph.getChildrenEvents(event);
      if (children != null) {
        var totalDurationOfExitSpans = children.stream()
            .filter(InternalServiceLatencyEnricher::isEligibleForSubtraction)
            .map(InternalServiceLatencyEnricher::getEventDuration)
            .reduce(0L, Long::sum);
        event.getAttributes().getAttributeMap()
            .put(EnrichedSpanConstants.INTERNAL_SVC_LATENCY, AttributeValueCreator.create(
                String.valueOf(parentDuration - totalDurationOfExitSpans)));
      }
    }
  }

  private static long getEventDuration(Event event) {
    return event.getEndTimeMillis() - event.getStartTimeMillis();
  }

  private static boolean isEligibleForSubtraction(Event childEvent) {
    var apiBoundaryType = childEvent.getEnrichedAttributes().getAttributeMap()
        .get(API_BOUNDARY_TYPE_ATTR_NAME);
    return apiBoundaryType != null && apiBoundaryType.getValue().equals(EXIT_BOUNDARY_TYPE);
  }
}
