package org.hypertrace.traceenricher.enrichment.enrichers;

import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.ApiNodeEventEdge;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;

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
                  EnrichedSpanConstants.API_EXIT_SERVICES_ATTRIBUTE,
                  AttributeValue.newBuilder()
                      .setValueMap(apiExitCallInfo.getCalleeNameToExitCalls())
                      .build());
            });
  }

  /**
   * An api_node represents an api call in the trace It consists of api_entry span and multiple
   * api_exit and internal spans
   *
   * <p>This method computes the count of exit calls for any given api (identified by
   * api_entry_span) This count is a composition of 2 things 1. link between api_exit_span in
   * api_node to api_entry_span (child of api_exit_span) in another api_node 2. exit calls to
   * backend from api_exit_span in api_node
   */
  Map<ByteBuffer, ApiExitCallInfo> computeApiExitCallCount(StructuredTrace trace) {
    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    // event -> api exit call count for the corresponding api_node
    Map<ByteBuffer, ApiExitCallInfo> eventToExitInfo = Maps.newHashMap();

    for (ApiNode<Event> apiNode : apiTraceGraph.getApiNodeList()) {
      List<Event> backendExitEvents =
          apiTraceGraph.getExitBoundaryEventsWithNoOutboundEdgeForApiNode(apiNode);
      List<ApiNodeEventEdge> edges = apiTraceGraph.getOutboundEdgesForApiNode(apiNode);
      int totalExitCallCount = backendExitEvents.size() + edges.size();
      ApiExitCallInfo apiExitCallInfo = new ApiExitCallInfo().withExitCallCount(totalExitCallCount);
      edges.forEach(edge -> apiExitCallInfo.handleApiNodeEdge(trace, edge));
      backendExitEvents.forEach(apiExitCallInfo::handleBackend);
      apiNode.getEvents().forEach(e -> eventToExitInfo.put(e.getEventId(), apiExitCallInfo));
    }
    return eventToExitInfo;
  }

  static class ApiExitCallInfo {

    private static final String UNKNOWN_SERVICE = "unknown-service";
    private static final String UNKNOWN_BACKEND = "unknown-backend";

    private final Map<String, String> calleeIdToName;
    private final Map<String, AtomicInteger> calleeIdToCallCount;

    private int exitCallCount;
    private int unknownServiceExits;
    private int unknownBackendExits;

    public ApiExitCallInfo() {
      unknownServiceExits = 0;
      unknownBackendExits = 0;
      this.calleeIdToName = Maps.newHashMap();
      this.calleeIdToCallCount = Maps.newHashMap();
    }

    public ApiExitCallInfo withExitCallCount(int exitCallCount) {
      this.exitCallCount = exitCallCount;
      return this;
    }

    void handleApiNodeEdge(StructuredTrace trace, ApiNodeEventEdge edge) {
      Event event = trace.getEventList().get(edge.getTgtEventIndex());
      if (EnrichedSpanUtils.containsServiceId(event)) {
        calleeIdToName.put(
            EnrichedSpanUtils.getServiceId(event), EnrichedSpanUtils.getServiceName(event));
        calleeIdToCallCount
            .computeIfAbsent(EnrichedSpanUtils.getServiceId(event), v -> new AtomicInteger(0))
            .incrementAndGet();
      } else {
        unknownServiceExits++;
      }
    }

    void handleBackend(Event exitEvent) {
      if (exitEvent
          .getEnrichedAttributes()
          .getAttributeMap()
          .containsKey(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_ID))) {
        calleeIdToName.put(
            EnrichedSpanUtils.getBackendId(exitEvent), EnrichedSpanUtils.getBackendName(exitEvent));
        calleeIdToCallCount
            .computeIfAbsent(EnrichedSpanUtils.getBackendId(exitEvent), v -> new AtomicInteger(0))
            .incrementAndGet();
      } else {
        unknownBackendExits++;
      }
    }

    int getExitCallCount() {
      return exitCallCount;
    }

    Map<String, String> getCalleeNameToExitCalls() {
      Map<String, String> calleeNameToExitCalls =
          calleeIdToCallCount.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      k -> calleeIdToName.get(k.getKey()),
                      v -> String.valueOf(v.getValue().get())));
      if (unknownServiceExits > 0) {
        calleeNameToExitCalls.put(UNKNOWN_SERVICE, String.valueOf(unknownServiceExits));
      }
      if (unknownBackendExits > 0) {
        calleeNameToExitCalls.put(UNKNOWN_BACKEND, String.valueOf(unknownBackendExits));
      }
      return calleeNameToExitCalls;
    }
  }
}
