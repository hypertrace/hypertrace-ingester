package org.hypertrace.traceenricher.trace.util;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
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

  private final StructuredTrace trace;
  private final List<ApiNode<Event>> apiNodeList;
  private final List<ApiNodeEventEdge> apiNodeEventEdgeList;
  private final Map<Integer, Set<Integer>> apiNodeIdxToEdges;
  private final Map<ByteBuffer, Integer> eventIdToIndexInTrace;
  // map of api node to index in api nodes list, since we want to build edges between api nodes
  private final Map<ApiNode<Event>, Integer> apiNodeToIndex;
  // map of entry boundary event to api node. each api node has one entry boundary event
  private final Map<Event, ApiNode<Event>> entryBoundaryToApiNode;
  private final Table<Integer, Integer, Edge> traceEdgeTable;
  private final Map<ByteBuffer, Integer> eventIdToApiNodeIndex;
  private final Set<Integer> apiExitBoundaryEventIdxWithNoOutgoingEdge;
  private final Set<Integer> apiExitBoundaryEventIdxWithOutgoingEdge;
  private final Set<Integer> apiEntryBoundaryEventIdxWithNoIncomingEdge;
  private final Set<Integer> apiEntryBoundaryEventIdxWithIncomingEdge;

  public ApiTraceGraph(StructuredTrace trace) {
    this.trace = trace;
    apiNodeList = Lists.newArrayList();
    apiNodeEventEdgeList = Lists.newArrayList();
    apiNodeIdxToEdges = Maps.newHashMap();
    eventIdToIndexInTrace = Maps.newHashMap();

    apiNodeToIndex = Maps.newHashMap();
    entryBoundaryToApiNode = Maps.newHashMap();
    traceEdgeTable = HashBasedTable.create();

    eventIdToApiNodeIndex = Maps.newHashMap();
    apiExitBoundaryEventIdxWithNoOutgoingEdge = Sets.newHashSet();
    apiExitBoundaryEventIdxWithOutgoingEdge = Sets.newHashSet();
    apiEntryBoundaryEventIdxWithNoIncomingEdge = Sets.newHashSet();
    apiEntryBoundaryEventIdxWithIncomingEdge = Sets.newHashSet();

    buildEventIdToIndexInTrace();
    buildTraceEdgeTable();
    buildApiTraceGraph();
    buildApiEntryBoundaryEventWithNoIncomingEdge();
    buildApiExitBoundaryEventWithNoOutgoingEdge();
  }

  public StructuredTrace getTrace() {
    return trace;
  }

  public List<ApiNode<Event>> getApiNodeList() {
    return apiNodeList;
  }

  public List<ApiNodeEventEdge> getApiNodeApiNodeEdgeList() {
    return apiNodeEventEdgeList;
  }

  public List<Event> getApiExitBoundaryEventsWithNoOutgoingEdge() {
    return getEventsForIndices(apiExitBoundaryEventIdxWithNoOutgoingEdge);
  }

  public List<Event> getApiExitBoundaryEventsWithOutgoingEdge() {
    return getEventsForIndices(apiExitBoundaryEventIdxWithOutgoingEdge);
  }

  public List<Event> getApiEntryBoundaryEventsWithNoIncomingEdge() {
    return getEventsForIndices(apiEntryBoundaryEventIdxWithNoIncomingEdge);
  }

  public List<Event> getApiEntryBoundaryEventsWithIncomingEdge() {
    return getEventsForIndices(apiEntryBoundaryEventIdxWithIncomingEdge);
  }

  public List<ApiNodeEventEdge> getOutboundEdgesForApiNode(
      ApiNode<Event> apiNode) {
    int idx = apiNodeToIndex.get(apiNode);
    if (!apiNodeIdxToEdges.containsKey(idx)) {
      return Collections.emptyList();
    }
    return apiNodeIdxToEdges.get(apiNodeToIndex.get(apiNode))
        .stream()
        .map(apiNodeEventEdgeList::get)
        .collect(Collectors.toList());
  }

  public List<Event> getExitBoundaryEventsWithNoOutboundEdgeForApiNode(
      ApiNode<Event> apiNode) {
    return apiNode.getExitApiBoundaryEvents().stream()
        .filter(v -> apiExitBoundaryEventIdxWithNoOutgoingEdge.contains(
            eventIdToIndexInTrace.get(v.getEventId())))
        .collect(Collectors.toList());
  }

  private void buildApiTraceGraph() {
    StructuredTraceGraph graph = StructuredTraceGraphBuilder.buildGraph(trace);

    buildApiNodes(graph);

    //optimization
    buildApiNodeToIndexMap(apiNodeList);

    buildEdge(graph, apiNodeList);
  }

  /**
   * The idea is to filter out the events in an API boundary. The ApiNode will contain a head event for the API Trace,
   * a list of events that fall within the API boundary, the Entry event or null if the head span is not an entry span
   * and a list of exit events from the API.
   */
  private void buildApiNodes(StructuredTraceGraph graph) {
    Set<ByteBuffer> remainingEventIds = new HashSet<>(graph.getEventMap().keySet());

    for (Event event : trace.getEventList()) {
      if (EnrichedSpanUtils.isEntryApiBoundary(event)) {
        // create new ApiNode from the events in the api boundary
        ApiNode<Event> apiNode = buildApiNode(graph, event);

        apiNodeList.add(apiNode);

        // Remove the events in ApiNode from remainingEventIds
        apiNode.getEvents().forEach(e -> {
          remainingEventIds.remove(e.getEventId());
          eventIdToApiNodeIndex.put(e.getEventId(), apiNodeList.size() - 1);
        });
      }
    }

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
          ApiNode<Event> apiNode = buildApiNode(graph, event);

          // We expect all events to be present in the remaining events here.
          Set<ByteBuffer> newEventIds = apiNode.getEvents().stream().map(Event::getEventId)
              .collect(
                  Collectors.toSet());
          Set<ByteBuffer> additionalEvents = Sets.difference(newEventIds, remainingEventIds);
          if (!additionalEvents.isEmpty()) {
            LOGGER.warn("Unexpected spans are included in ApiNode; additionalSpans: {}",
                additionalEvents.stream().map(HexUtils::getHex).collect(Collectors.toSet()));
          }

          apiNodeList.add(apiNode);
          apiNode.getEvents().forEach(e -> remainingEventIds.remove(e.getEventId()));
        } else if (!StringUtils.equals(EnrichedSpanUtils.getSpanType(event), UNKNOWN_SPAN_KIND_VALUE)) {
          LOGGER.warn("Non exit root span wasn't picked for ApiNode; traceId: {}, spanId: {}, spanName: {}, serviceName: {}",
              HexUtils.getHex(trace.getTraceId()),
              HexUtils.getHex(event.getEventId()),
              event.getEventName(),
              event.getServiceName()
          );
        }
      }
    }

    if (!remainingEventIds.isEmpty() && LOGGER.isDebugEnabled()) {
      LOGGER.debug("Not all spans from trace are included in ApiNodes; traceId: {}, spanIds: {}",
          HexUtils.getHex(trace.getTraceId()),
          remainingEventIds.stream().map(HexUtils::getHex).collect(Collectors.toSet()));
    }
  }

  /**
   * Traverse events starting from the {@code rootEvent}
   * and find out all the api boundary events it leads to
   */
  private ApiNode<Event> buildApiNode(
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

    return new ApiNode<>(
        rootEvent, events,
        (EnrichedSpanUtils.isEntryApiBoundary(rootEvent) ? rootEvent : null),
        exitApiBoundaryEvents);
  }

  private void buildApiNodeToIndexMap(List<ApiNode<Event>> apiNodes) {
    for (int i = 0; i < apiNodes.size(); i++) {
      ApiNode<Event> apiNode = apiNodes.get(i);
      apiNode.getEntryApiBoundaryEvent().ifPresent(e -> entryBoundaryToApiNode.put(e, apiNode));
      apiNodeToIndex.put(apiNode, i);
    }
  }

  private void buildEdge(
      StructuredTraceGraph graph, List<ApiNode<Event>> apiNodes) {
    // 1. get all the exit boundary events from an api node
    // 2. exit boundary events are the only ones which can call a different api node
    // 3. find all the children of exit boundary events, which will be entry boundary nodes of different api nodes
    // 4. find all the api nodes based on children of exit boundary events from `entryBoundaryToApiNode`
    // 5. connect the exit boundary and entry boundary of different api node with an edge
    for (ApiNode<Event> apiNode : apiNodes) {
      //exit boundary events of api node
      List<Event> exitBoundaryEvents = apiNode.getExitApiBoundaryEvents();
      for (Event exitBoundaryEvent : exitBoundaryEvents) {
        List<Event> exitBoundaryEventChildren = graph.getChildrenEvents(exitBoundaryEvent);
        if (exitBoundaryEventChildren != null) {
          for (Event exitBoundaryEventChild : exitBoundaryEventChildren) {
            // if the child of an exit boundary event is entry api boundary type, which should be always!
            if (EnrichedSpanUtils.isEntryApiBoundary(exitBoundaryEventChild)) {
              // get the api node exit boundary event is connecting to
              ApiNode<Event> destinationApiNode = entryBoundaryToApiNode.get(exitBoundaryEventChild);

              Optional<ApiNodeEventEdge> edgeBetweenApiNodes = createEdgeBetweenApiNodes(
                  apiNode, destinationApiNode, exitBoundaryEvent, exitBoundaryEventChild);
              edgeBetweenApiNodes.ifPresent(edge -> {
                apiNodeEventEdgeList.add(edge);
                apiExitBoundaryEventIdxWithOutgoingEdge.add(edge.getSrcEventIndex());
                apiEntryBoundaryEventIdxWithIncomingEdge.add(edge.getTgtEventIndex());
                apiNodeIdxToEdges.computeIfAbsent(
                    apiNodeToIndex.get(apiNode), v -> Sets.newHashSet()).add(apiNodeEventEdgeList.size() - 1);
              });
            } else {
              LOGGER.warn("Exit boundary event with eventId: {}, eventName: {}, serviceName: {}," +
                      " can only have entry boundary event as child. Non-entry child:" +
                      " childEventId: {}, childEventName: {}, childServiceName: {}." +
                      " traceId for events: {}",
                  HexUtils.getHex(exitBoundaryEvent.getEventId()),
                  exitBoundaryEvent.getEventName(),
                  exitBoundaryEvent.getServiceName(),
                  HexUtils.getHex(exitBoundaryEventChild.getEventId()),
                  exitBoundaryEventChild.getEventName(),
                  exitBoundaryEventChild.getServiceName(),
                  HexUtils.getHex(trace.getTraceId())
              );
            }
          }
        }
      }
    }
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
          exitBoundaryEventFromSrcApiNode.getEventId());
      Integer targetIndexInTrace = eventIdToIndexInTrace.get(
          entryBoundaryEventOfDestinationApiNode.getEventId());

      Edge edgeInTrace = traceEdgeTable.get(srcIndexInTrace, targetIndexInTrace);

      if (null == edgeInTrace) {
        LOGGER.warn("There should be an edge between src event {} and target event {}",
            exitBoundaryEventFromSrcApiNode, entryBoundaryEventOfDestinationApiNode);
      } else {
        ApiNodeEventEdge apiNodeEventEdge = ApiNodeEventEdge.newBuilder()
            .setSrcApiNodeIndex(srcIndex)
            .setTgtApiNodeIndex(targetIndex)
            .setSrcEventIndex(srcIndexInTrace)
            .setTgtEventIndex(targetIndexInTrace)
            .setAttributes(edgeInTrace.getAttributes())
            .setMetrics(edgeInTrace.getMetrics())
            .setStartTimeMillis(edgeInTrace.getStartTimeMillis())
            .setEndTimeMillis(edgeInTrace.getEndTimeMillis())
            .build();

        return Optional.of(apiNodeEventEdge);
      }
    } else {
      LOGGER.warn("Strange! Entry boundary event {} should already have been discovered as an api node",
          entryBoundaryEventOfDestinationApiNode);
    }
    return Optional.empty();
  }

  private void buildApiEntryBoundaryEventWithNoIncomingEdge() {
    apiNodeList.stream()
        .map(ApiNode::getEntryApiBoundaryEvent)
        .filter(Optional::isPresent)
        .map(e -> eventIdToIndexInTrace.get(e.get().getEventId()))
        .filter(eventIdx -> !apiEntryBoundaryEventIdxWithIncomingEdge.contains(eventIdx))
        .forEach(apiEntryBoundaryEventIdxWithNoIncomingEdge::add);
  }

  private void buildApiExitBoundaryEventWithNoOutgoingEdge() {
    apiNodeList.stream()
        .flatMap(apiNode -> apiNode.getExitApiBoundaryEvents().stream())
        .map(e -> eventIdToIndexInTrace.get(e.getEventId()))
        .filter(eventIdx -> !apiExitBoundaryEventIdxWithOutgoingEdge.contains(eventIdx))
        .forEach(apiExitBoundaryEventIdxWithNoOutgoingEdge::add);
  }

  private void buildTraceEdgeTable() {
    for (Edge edge : trace.getEventEdgeList()) {
      traceEdgeTable.put(edge.getSrcIndex(), edge.getTgtIndex(), edge);
    }
  }

  private void buildEventIdToIndexInTrace() {
    int count = 0;
    for (Event event : trace.getEventList()) {
      eventIdToIndexInTrace.put(event.getEventId(), count++);
    }
  }

  private List<Event> getEventsForIndices(Set<Integer> set) {
    return set.stream()
        .map(v -> trace.getEventList().get(v))
        .collect(Collectors.toList());

  }
}
