package org.hypertrace.traceenricher.enrichment.enrichers;

import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.ApiNodeEventEdge;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraphBuilder;

public class ExitCallsEnricher extends AbstractTraceEnricher {

  @Override
  public void enrichTrace(StructuredTrace trace) {
    Map<ByteBuffer, ApiExitCallInfo> exitCallInfo = computeApiExitCallCount(trace);
    trace.getEventList().stream()
        .filter(event -> exitCallInfo.containsKey(event.getEventId()))
        .forEach(
            event -> {
              ApiExitCallInfo apiExitCallInfo = exitCallInfo.get(event.getEventId());
              addEnrichedAttribute(
                  event,
                  EnrichedSpanConstants.API_EXIT_CALLS_ATTRIBUTE,
                  AttributeValueCreator.create(apiExitCallInfo.getExitCallCount()));
              addEnrichedAttribute(
                  event,
                  EnrichedSpanConstants.API_CALLEE_NAME_COUNT_ATTRIBUTE,
                  AttributeValue.newBuilder()
                      .setValueMap(apiExitCallInfo.getCalleeNameToCallCount())
                      .build());
            });
  }

  /**
   * An api_node represents an api call in the trace It consists of api_entry span and multiple
   * api_exit and internal spans
   *
   * <p>This method computes the count & breakup of exit calls for any given api (identified by
   * api_entry_span) This count is a composition of 2 things
   *
   * <ul>
   *   <li>1. link between api_exit_span in api_node to api_entry_span (child of api_exit_span) in
   *       another api_node
   *   <li>2. exit calls to backend from api_exit_span in api_node
   * </ul>
   */
  Map<ByteBuffer, ApiExitCallInfo> computeApiExitCallCount(StructuredTrace trace) {
    ApiTraceGraph apiTraceGraph = ApiTraceGraphBuilder.buildGraph(trace);
    // event -> api exit call count for the corresponding api_node
    Map<ByteBuffer, ApiExitCallInfo> eventToExitInfo = Maps.newHashMap();

    int totalTraceExitCallCount = 0;

    for (ApiNode<Event> apiNode : apiTraceGraph.getApiNodeList()) {
      List<Event> backendExitEvents =
          apiTraceGraph.getExitBoundaryEventsWithNoOutboundEdgeForApiNode(apiNode);
      List<ApiNodeEventEdge> edges = apiTraceGraph.getOutboundEdgesForApiNode(apiNode);
      int totalExitCallCount = backendExitEvents.size() + edges.size();
      ApiExitCallInfo apiExitCallInfo = new ApiExitCallInfo().withExitCallCount(totalExitCallCount);
      totalTraceExitCallCount += totalExitCallCount;
      edges.forEach(edge -> apiExitCallInfo.handleApiNodeEdge(trace, edge));
      backendExitEvents.forEach(apiExitCallInfo::handleBackend);
      apiNode.getEvents().forEach(e -> eventToExitInfo.put(e.getEventId(), apiExitCallInfo));
    }

    addTotalNumberOfApiCallsEnrichedAttribute(apiTraceGraph, totalTraceExitCallCount);

    return eventToExitInfo;
  }

  private void addTotalNumberOfApiCallsEnrichedAttribute(
      ApiTraceGraph apiTraceGraph, int totalTraceExitCallCount) {
    if (!apiTraceGraph.getApiNodeList().isEmpty()
        && apiTraceGraph.getApiNodeList().get(0).getHeadEvent() != null
     ) {
      addEnrichedAttribute(
          apiTraceGraph.getApiNodeList().get(0).getHeadEvent(),
          EnrichedSpanConstants.TOTAL_NUMBER_OF_API_EXIT_CALLS,
          AttributeValueCreator.create(totalTraceExitCallCount));
    }
  }

  static class ApiExitCallInfo {

    private static final String UNKNOWN_SERVICE = "unknown-service";
    private static final String UNKNOWN_BACKEND = "unknown-backend";

    private final Map<String, AtomicInteger> calleeNameToCallCount;

    private int exitCallCount;

    public ApiExitCallInfo() {
      this.calleeNameToCallCount = Maps.newHashMap();
    }

    public ApiExitCallInfo withExitCallCount(int exitCallCount) {
      this.exitCallCount = exitCallCount;
      return this;
    }

    void handleApiNodeEdge(StructuredTrace trace, ApiNodeEventEdge edge) {
      Event event = trace.getEventList().get(edge.getTgtEventIndex());
      String serviceName = EnrichedSpanUtils.getServiceName(event);
      calleeNameToCallCount
          .computeIfAbsent(
              Objects.requireNonNullElse(serviceName, UNKNOWN_SERVICE), v -> new AtomicInteger(0))
          .incrementAndGet();
    }

    void handleBackend(Event exitEvent) {
      String backendName = EnrichedSpanUtils.getBackendName(exitEvent);
      calleeNameToCallCount
          .computeIfAbsent(
              Objects.requireNonNullElse(backendName, UNKNOWN_BACKEND), v -> new AtomicInteger(0))
          .incrementAndGet();
    }

    int getExitCallCount() {
      return exitCallCount;
    }

    Map<String, String> getCalleeNameToCallCount() {
      return calleeNameToCallCount.entrySet().stream()
          .collect(Collectors.toMap(Entry::getKey, v -> String.valueOf(v.getValue().get())));
    }
  }
}
