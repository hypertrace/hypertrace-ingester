package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants.HEAD_EVENT_ID;
import static org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants.TOTAL_NUMBER_OF_TRACE_CALLS;
import static org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants.TOTAL_NUMBER_OF_UNIQUE_API_NODES;

import java.util.List;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
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
    addTotalAmountOfCallsToHeadSpanAttribute(trace, firstNodeHeadSpan);
    addTotalNumberOfUniqueApiNodesHeadSpanAttribute(
        apiTraceGraph.getApiNodeList(), firstNodeHeadSpan);
  }

  private void addHeadSpanIdTraceAttribute(StructuredTrace trace, Event headSpan) {
    AttributeValue attribute =
        AttributeValue.newBuilder().setBinaryValue(headSpan.getEventId()).build();
    trace.getAttributes().getAttributeMap().put(HEAD_EVENT_ID, attribute);
  }

  private void addTotalAmountOfCallsToHeadSpanAttribute(StructuredTrace trace, Event headSpan) {
    if (!trace.getEventEdgeList().isEmpty()) {
      headSpan
          .getEnrichedAttributes()
          .getAttributeMap()
          .put(
              TOTAL_NUMBER_OF_TRACE_CALLS,
              AttributeValueCreator.create(trace.getEventEdgeList().size()));
    }
  }

  private void addTotalNumberOfUniqueApiNodesHeadSpanAttribute(
      List<ApiNode<Event>> apiNodeList, Event headSpan) {
    headSpan
        .getEnrichedAttributes()
        .getAttributeMap()
        .put(TOTAL_NUMBER_OF_UNIQUE_API_NODES, AttributeValueCreator.create(apiNodeList.size()));
  }
}
