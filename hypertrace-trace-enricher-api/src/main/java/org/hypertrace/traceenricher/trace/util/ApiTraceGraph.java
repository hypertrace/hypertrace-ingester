package org.hypertrace.traceenricher.trace.util;

import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.core.datamodel.ApiNodeEventEdge;
import org.hypertrace.core.datamodel.Edge;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.AttributeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiTraceGraph {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiTraceGraph.class);
  private static final String UNKNOWN_SPAN_KIND_VALUE =
      EnrichedSpanConstants.getValue(AttributeValue.ATTRIBUTE_VALUE_UNKNOWN);

  private List<ApiNode<Event>> nodeList;
  private List<ApiNodeEventEdge> apiNodeEventEdgeList;
  private StructuredTrace trace;
  private Map<String, Integer> eventIdToIndexInTrace;

  public ApiTraceGraph(StructuredTrace trace) {
    this.trace = trace;
    nodeList = new ArrayList<>();
    apiNodeEventEdgeList = new ArrayList<>();
    eventIdToIndexInTrace = buildEventIdToIndexInTrace(trace);
  }

  public List<ApiNode<Event>> getNodeList() {
    return nodeList;
  }

  public List<ApiNodeEventEdge> getApiNodeEventEdgeList() {
    return apiNodeEventEdgeList;
  }

  public void setNodeList(List<ApiNode<Event>> nodeList) {
    this.nodeList = nodeList;
  }

  // map of api node to index in api nodes list, since we want to build edges between api nodes
  private Map<ApiNode<Event>, Integer> apiNodeToIndex;
  //map of entry boundary event to api node. each api node has one entry boundary event
  private Map<Event, ApiNode<Event>> entryBoundaryToApiNode;

  /**
   * The idea is to filter out the events in an API boundary. The ApiNode will contain a head event for the API Trace,
   * a list of events that fall within the API boundary, the Entry event or null if the head span is not an entry span
   * and a list of exit events from the API.
   */
  private List<ApiNode<Event>> buildApiNodes(StructuredTraceGraph graph) {
    List<ApiNode<Event>> apiNodes = new ArrayList<>();
    Set<ByteBuffer> processedEvents = new HashSet<>();

    for (Event event : trace.getEventList()) {
      if (EnrichedSpanUtils.isEntryApiBoundary(event)) {
        // get all the events in the api boundary
        Pair<List<Event>, List<Event>> pair = getEventsInApiBoundary(graph, event);

        // create new ApiNode from the events in the api boundary
        apiNodes.add(new ApiNode<>(event, pair.getLeft(), event, pair.getRight()));

        // Add these events to the processed events.
        processedEvents.add(event.getEventId());
        pair.getLeft().forEach(e -> processedEvents.add(e.getEventId()));
      }
    }

    // Now check the list of events which aren't processed, which belong to different apiNodes.
    Set<ByteBuffer> remainingEventIds = new HashSet<>(
        Sets.difference(graph.getEventMap().keySet(), processedEvents));
    if (!remainingEventIds.isEmpty()) {
      // Process all the roots which aren't processed yet.
      for (Event event : graph.getRootEvents()) {
        if (!remainingEventIds.contains(event.getEventId())) {
          continue;
        }

        // If the span is an exit span, then this is a separate root and could happen when
        // some intermediate spans were missing (broken trace) or this could the case where
        // the caller only did exit call without an incoming entry (client only).
        // TODO: What if the root is an internal span?
        if (EnrichedSpanUtils.isExitSpan(event)) {
          // Get all the spans that should be included in this ApiNode and create new node.
          Pair<List<Event>, List<Event>> pair = getEventsInApiBoundary(graph, event);

          // We expect all events to be present in the remaining events here.
          Set<ByteBuffer> newEventIds = pair.getLeft().stream().map(Event::getEventId).collect(
              Collectors.toSet());
          Set<ByteBuffer> additionalEvents = Sets.difference(newEventIds, remainingEventIds);
          if (!additionalEvents.isEmpty()) {
            LOGGER.warn("Unexpected spans are included in ApiNode; additionalSpans: {}",
                additionalEvents.stream().map(HexUtils::getHex).collect(Collectors.toSet()));
          }

          apiNodes.add(new ApiNode<>(event, pair.getLeft(), null, pair.getRight()));
          pair.getLeft().forEach(e -> remainingEventIds.remove(e.getEventId()));
          remainingEventIds.remove(event.getEventId());
        } else if (!StringUtils.equals(EnrichedSpanUtils.getSpanType(event), UNKNOWN_SPAN_KIND_VALUE)) {
          LOGGER.warn("Non exit root span wasn't picked for ApiNode; traceId: {}, span: {}",
              HexUtils.getHex(trace.getTraceId()), event);
        }
      }
    }

    if (!remainingEventIds.isEmpty() && LOGGER.isDebugEnabled()) {
      LOGGER.debug("Not all spans from trace are included in ApiNodes; traceId: {}, spanIds: {}",
          HexUtils.getHex(trace.getTraceId()),
          remainingEventIds.stream().map(HexUtils::getHex).collect(Collectors.toSet()));
    }

    return apiNodes;
  }

  private Pair<List<Event>, List<Event>> getEventsInApiBoundary(
      StructuredTraceGraph graph, Event rootEvent) {
    List<Event> events = new ArrayList<>();
    events.add(rootEvent);

    List<Event> exitApiBoundaryEvents = new ArrayList<>();
    if (EnrichedSpanUtils.isExitApiBoundary(rootEvent)) {
      exitApiBoundaryEvents.add(rootEvent);
    }

    Queue<Event> q = new LinkedList<>();
    q.add(rootEvent);

    while (!q.isEmpty()) {
      Event e = q.remove();
      // This is the main logic of filtering out events which lies in an API boundary.
      // Rest of the code is just filtering out entities and edges based on the events in API boundary

      // get all the children of `e`
      // 1. if there is a child which is exit boundary, we have hit a corresponding exit boundary.
      // We should not process the children of exit boundary span
      // 2. if the child is an entry boundary, we have a new boundary. Ignore this child
      // 3. if the children of `e` is null, add `e` to the api boundary
      List<Event> children = graph.getChildrenEvents(e);
      if (children != null) {
        for (Event child : children) {
          if (EnrichedSpanUtils.isExitApiBoundary(child)) {
            events.add(child);
            exitApiBoundaryEvents.add(child);
          } else if (EnrichedSpanUtils.isEntryApiBoundary(child)) {
            //a new api boundary. don't do anything
          } else {
            q.add(child);
            // an intermediate event. intermediate events are inside API boundary
            events.add(child);
          }
        }
      }
    }

    return Pair.of(events, exitApiBoundaryEvents);
  }

  public ApiTraceGraph build() {
    StructuredTraceGraph graph = StructuredTraceGraph.createGraph(trace);

    List<ApiNode<Event>> apiNodes = buildApiNodes(graph);
    this.setNodeList(apiNodes);

    //optimization
    buildApiNodeToIndexMap(apiNodes);

    //1. get all the exit boundary events from an api node
    //2. exit boundary events are the only ones which can call a different api node
    //3. find all the children of exit boundary events, which will be entry boundary nodes of different api nodes
    //4. find all the api nodes based on children of exit boundary events from `entryBoundaryToApiNode`
    //5. connect the exit boundary and entry boundary of different api node with an edge
    for (ApiNode<Event> apiNode : apiNodes) {
      //exit boundary events of api node
      List<Event> exitBoundaryEvents = apiNode.getExitApiBoundaryEvents();
      for (Event exitBoundaryEvent : exitBoundaryEvents) {
        List<Event> children = graph.getChildrenEvents(exitBoundaryEvent);
        if (children != null) {
          for (Event child : children) {
            // if the child of an exit boundary event is entry api boundary type, which should be always!
            if (EnrichedSpanUtils.isEntryApiBoundary(child)) {
              // get the api node exit boundary event is connecting to
              ApiNode<Event> destinationApiNode = entryBoundaryToApiNode.get(child);

              Optional<ApiNodeEventEdge> edgeBetweenApiNodes = createEdgeBetweenApiNodes(apiNode, destinationApiNode,
                  exitBoundaryEvent, child);
              edgeBetweenApiNodes.ifPresent(apiNodeEventEdgeList::add);
            } else {
              LOGGER.warn("Exit boundary event with eventId: {}, eventName: {}, serviceName: {}," +
                      " can only have entry boundary event as child. Non-entry child:" +
                      " childEventId: {}, childEventName: {}, childServiceName: {}." +
                      " traceId for events: {}",
                  HexUtils.getHex(exitBoundaryEvent.getEventId()),
                  exitBoundaryEvent.getEventName(),
                  exitBoundaryEvent.getServiceName(),
                  HexUtils.getHex(child.getEventId()),
                  child.getEventName(),
                  child.getServiceName(),
                  HexUtils.getHex(trace.getTraceId())
              );
            }
          }
        }
      }
    }
    return this;
  }

  private Optional<ApiNodeEventEdge> createEdgeBetweenApiNodes(
      ApiNode<Event> srcApiNode,
      ApiNode<Event> destinationApiNode,
      Event exitBoundaryEventFromSrcApiNode,
      Event entryBoundaryEventOfDestinationApiNode) {
    if (destinationApiNode != null) {
      // get the indexes in apiNodes list to create an edge
      Integer srcIndex = apiNodeToIndex.get(srcApiNode);
      Integer targetIndex = apiNodeToIndex.get(destinationApiNode);

      //Get the actual edge from trace connecting exitBoundaryEvent and child
      Integer srcIndexInTrace = eventIdToIndexInTrace.get(
          HexUtils.getHex(exitBoundaryEventFromSrcApiNode.getEventId()));
      Integer targetIndexInTrace = eventIdToIndexInTrace.get(
          HexUtils.getHex(entryBoundaryEventOfDestinationApiNode.getEventId()));

      Optional<Edge> edgeInTrace = getEdgeFromTrace(trace, srcIndexInTrace, targetIndexInTrace);

      if (edgeInTrace.isEmpty()) {
        LOGGER.warn("There should be an edge between src event {} and target event {}",
            exitBoundaryEventFromSrcApiNode, entryBoundaryEventOfDestinationApiNode);
      } else {
        ApiNodeEventEdge apiNodeEventEdge = ApiNodeEventEdge.newBuilder()
            .setSrcApiNodeIndex(srcIndex)
            .setTgtApiNodeIndex(targetIndex)
            .setSrcEventIndex(srcIndexInTrace)
            .setTgtEventIndex(targetIndexInTrace)
            .setAttributes(edgeInTrace.get().getAttributes())
            .setMetrics(edgeInTrace.get().getMetrics())
            .setStartTimeMillis(edgeInTrace.get().getStartTimeMillis())
            .setEndTimeMillis(edgeInTrace.get().getEndTimeMillis())
            .build();

        return Optional.of(apiNodeEventEdge);
      }
    } else {
      LOGGER.warn("Strange! Entry boundary event {} should already have been discovered as an api node",
          entryBoundaryEventOfDestinationApiNode);
    }
    return Optional.empty();
  }

  private void buildApiNodeToIndexMap(List<ApiNode<Event>> apiNodes) {
    entryBoundaryToApiNode = new HashMap<>();
    apiNodeToIndex = new HashMap<>();
    for (int i = 0; i < apiNodes.size(); i++) {
      ApiNode<Event> apiNode = apiNodes.get(i);
      apiNode.getEntryApiBoundaryEvent().ifPresent(e -> entryBoundaryToApiNode.put(e, apiNode));
      apiNodeToIndex.put(apiNode, i);
    }
  }

  private Optional<Edge> getEdgeFromTrace(StructuredTrace trace, Integer srcIndex, Integer targetIndex) {
    for (Edge edge : trace.getEventEdgeList()) {
      if (srcIndex.equals(edge.getSrcIndex()) && targetIndex.equals(edge.getTgtIndex())) {
        return Optional.of(edge);
      }
    }
    return Optional.empty();
  }

  private Map<String, Integer> buildEventIdToIndexInTrace(StructuredTrace trace) {
    Map<String, Integer> eventIdToEvent = new HashMap<>();
    // avoid autoboxing by specifying as object
    Integer count = 0;
    for (Event event : trace.getEventList()) {
      String eventId = HexUtils.getHex(event.getEventId());
      eventIdToEvent.put(eventId, count);
      count++;
    }
    return eventIdToEvent;
  }
}
