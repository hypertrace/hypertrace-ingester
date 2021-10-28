package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
import static org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants.HEAD_EVENT_ID;
import static org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants.UNIQUE_API_NODES_COUNT;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraphBuilder;

public class TraceStatsEnricher extends AbstractTraceEnricher {

  @Override
  public void enrichTrace(StructuredTrace trace) {
    ApiTraceGraph apiTraceGraph = ApiTraceGraphBuilder.buildGraph(trace);
    if (apiTraceGraph.getApiNodeList().isEmpty()) {
      return;
    }
    Event firstNodeHeadSpan = apiTraceGraph.getApiNodeList().get(0).getHeadEvent();
    if (firstNodeHeadSpan == null) {
      return;
    }
    addHeadSpanIdTraceAttribute(trace, firstNodeHeadSpan);
    addUniqueApiNodesCountTraceAttribute(apiTraceGraph.getApiNodeList(), trace);
  }

  private void addHeadSpanIdTraceAttribute(StructuredTrace trace, Event headSpan) {
    AttributeValue attribute =
        fastNewBuilder(AttributeValue.Builder.class).setBinaryValue(headSpan.getEventId()).build();
    trace.getAttributes().getAttributeMap().put(HEAD_EVENT_ID, attribute);
  }

  private void addUniqueApiNodesCountTraceAttribute(
      List<ApiNode<Event>> apiNodeList, StructuredTrace trace) {
    Set<String> uniqueApiIds =
        apiNodeList.stream()
            .filter(eventApiNode -> eventApiNode.getEntryApiBoundaryEvent().isPresent())
            .map(
                eventApiNode ->
                    EnrichedSpanUtils.getApiId(eventApiNode.getEntryApiBoundaryEvent().get()))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    trace
        .getAttributes()
        .getAttributeMap()
        .put(UNIQUE_API_NODES_COUNT, AttributeValueCreator.create(uniqueApiIds.size()));
  }
}
