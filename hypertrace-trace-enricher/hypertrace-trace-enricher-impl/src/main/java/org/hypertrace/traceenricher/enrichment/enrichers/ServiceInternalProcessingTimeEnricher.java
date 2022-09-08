package org.hypertrace.traceenricher.enrichment.enrichers;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.ApiNodeEventEdge;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraphBuilder;
import org.slf4j.LoggerFactory;

public class ServiceInternalProcessingTimeEnricher extends AbstractTraceEnricher {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(ServiceInternalProcessingTimeEnricher.class);

  public void enrichTrace(StructuredTrace trace) {
    try {
      ApiTraceGraph apiTraceGraph = ApiTraceGraphBuilder.buildGraph(trace);
      List<ApiNode<Event>> apiNodeList = apiTraceGraph.getApiNodeList();
      for (ApiNode<Event> apiNode : apiNodeList) {
        List<Event> exitApiBoundaryEvents = apiNode.getExitApiBoundaryEvents();
        List<ApiNodeEventEdge> edges = apiTraceGraph.getOutboundEdgesForApiNode(apiNode);
        int edgeDurationSum = 0;
        // Note: this logic of summing the duration of each child span does not work if children
        // spans
        // were
        // concurrent to one-another. In that case, the parent span waits only for
        // max(duration_child_1,
        // duration_child2,...,duration_child_n) and not duration_child1 + duration_child_2 +
        // duration_child_3
        // Works for:
        // |------------------PARENT-------------------|
        //  |---C1---|
        //              |---C2---|
        //                          |---C3---|
        // Doesn't work for:
        // |------------------PARENT-------------------|
        //  |---C1---|
        //    |---C2---|
        //      |---C3---|
        for (var edge : edges) {
          edgeDurationSum += getApiNodeEventEdgeDuration(edge);
        }
        // now sum up http or https backends
        double httpExitCallsSum =
            exitApiBoundaryEvents.stream()
                .filter(
                    event -> {
                      Map<String, AttributeValue> enrichedAttributes =
                          event.getEnrichedAttributes().getAttributeMap();
                      return enrichedAttributes.containsKey("BACKEND_PROTOCOL")
                          && enrichedAttributes.get("BACKEND_PROTOCOL").getValue().contains("HTTP");
                    })
                .mapToDouble(event -> event.getMetrics().getMetricMap().get("Duration").getValue())
                .sum();
        Optional<Event> entryApiBoundaryEventMaybe = apiNode.getEntryApiBoundaryEvent();
        if (entryApiBoundaryEventMaybe.isPresent()) {
          var entryApiBoundaryEvent = entryApiBoundaryEventMaybe.get();
          var entryApiBoundaryEventDuration = getEventDuration(entryApiBoundaryEvent);
          try {
            entryApiBoundaryEvent
                .getAttributes()
                .getAttributeMap()
                .put(
                    EnrichedSpanConstants.INTERNAL_SVC_LATENCY,
                    AttributeValueCreator.create(
                        String.valueOf(
                            entryApiBoundaryEventDuration - edgeDurationSum - httpExitCallsSum)));
          } catch (NullPointerException e) {
            LOG.error(
                "NPE while calculating service internal time. entryApiBoundaryEventDuration {}, edgeDurationSum {}",
                entryApiBoundaryEventDuration,
                edgeDurationSum,
                e);
            throw e;
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Exception while calculating service internal time");
    }
  }

  private static Double getEventDuration(Event event) {
    assert event.getMetrics().getMetricMap() != null;
    assert event.getMetrics().getMetricMap().containsKey("Duration");
    return event.getMetrics().getMetricMap().get("Duration").getValue();
  }

  private static Double getApiNodeEventEdgeDuration(ApiNodeEventEdge edge) {
    assert edge.getMetrics().getMetricMap() != null;
    assert edge.getMetrics().getMetricMap().containsKey("Duration");
    return edge.getMetrics().getMetricMap().get("Duration").getValue();
  }
}
