package org.hypertrace.traceenricher.enrichment.enrichers;

import java.util.List;
import java.util.Optional;
import org.hypertrace.core.datamodel.ApiNodeEventEdge;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraphBuilder;

public class ServiceInternalProcessingTimeEnricher extends AbstractTraceEnricher {

  public void enrichTrace(StructuredTrace trace) {
    ApiTraceGraph apiTraceGraph = ApiTraceGraphBuilder.buildGraph(trace);
    List<ApiNode<Event>> apiNodeList = apiTraceGraph.getApiNodeList();
    for (ApiNode<Event> apiNode : apiNodeList) {
      List<ApiNodeEventEdge> edges = apiTraceGraph.getOutboundEdgesForApiNode(apiNode);
      int edgeDurationSum = 0;
      //Note: this logic of summing the duration of each child span does not work if children spans were
      // concurrent to one-another. In that case, the parent span waits only for max(duration_child_1,
      // duration_child2,...,duration_child_n) and not duration_child1 + duration_child_2 + duration_child_3
      //Works for:
      //|------------------PARENT-------------------|
      //  |---C1---|
      //              |---C2---|
      //                          |---C3---|
      //Doesn't work for:
      //|------------------PARENT-------------------|
      //  |---C1---|
      //    |---C2---|
      //      |---C3---|
      for (var edge : edges) {
        edgeDurationSum += edge.getEndTimeMillis() - edge.getStartTimeMillis();
      }
      Optional<Event> entryApiBoundaryEventMaybe = apiNode.getEntryApiBoundaryEvent();
      if (entryApiBoundaryEventMaybe.isPresent()) {
        var entryApiBoundaryEvent = entryApiBoundaryEventMaybe.get();
        var entryApiBoundaryEventDuration =
            entryApiBoundaryEvent.getEndTimeMillis() - entryApiBoundaryEvent.getStartTimeMillis();
        entryApiBoundaryEvent.getAttributes().getAttributeMap()
            .put(EnrichedSpanConstants.INTERNAL_SVC_LATENCY, AttributeValueCreator.create(
                String.valueOf(entryApiBoundaryEventDuration - edgeDurationSum)));
      }
    }
  }
}
