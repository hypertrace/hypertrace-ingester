package org.hypertrace.traceenricher.trace.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Edge;
import org.hypertrace.core.datamodel.EdgeType;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.junit.jupiter.api.Test;

public class ApiTraceGraphTest {

  public static final String TEST_CUSTOMER_ID = "testCustomerId";

  @Test
  public void testApiTraceGraph_HotrodTrace() throws IOException {
    URL resource =
        Thread.currentThread().getContextClassLoader().getResource("StructuredTrace-Hotrod.avro");

    SpecificDatumReader<StructuredTrace> datumReader =
        new SpecificDatumReader<>(StructuredTrace.getClassSchema());
    DataFileReader<StructuredTrace> dfrStructuredTrace =
        new DataFileReader<>(new File(resource.getPath()), datumReader);
    StructuredTrace trace = dfrStructuredTrace.next();
    dfrStructuredTrace.close();

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    assertEquals(12, apiTraceGraph.getApiNodeEventEdgeList().size());
    assertEquals(13, apiTraceGraph.getApiNodeList().size());
    assertNotNull(apiTraceGraph.getTrace());
    verifyEveryEventPartOfSingleApiNode_HotrodTrace(trace, apiTraceGraph);
  }

  private void verifyEveryEventPartOfSingleApiNode_HotrodTrace(
      StructuredTrace trace, ApiTraceGraph apiTraceGraph) {
    Map<ByteBuffer, Set<Integer>> eventToApiNodes = Maps.newHashMap();

    for (int index = 0; index < apiTraceGraph.getApiNodeList().size(); index++) {
      ApiNode<Event> apiNode = apiTraceGraph.getApiNodeList().get(index);
      int finalIndex = index;
      apiNode
          .getEvents()
          .forEach(
              v ->
                  eventToApiNodes
                      .computeIfAbsent(v.getEventId(), s -> new HashSet<>())
                      .add(finalIndex));
    }

    // verify every event belongs to exactly 1 api_node
    trace
        .getEventList()
        .forEach(
            e -> {
              assertTrue(eventToApiNodes.containsKey(e.getEventId()));
              assertEquals(1, eventToApiNodes.get(e.getEventId()).size());
            });
  }

  @Test
  void headSpanIndexInTraceIsAddedToTraceAttribute() {
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 0
    Event aExitEvent = createExitEventName("aExitEvent"); // 1
    Event bEntryEvent = createEntryEventWithName("bEvent"); // 2

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{aEntryHeadSpanEvent, aExitEvent, bEntryEvent},
            new HashMap<>() {
              {
                put(0, new int[]{1});
                put(1, new int[]{2});
              }
            });

    new ApiTraceGraph(trace);
    String actualHeadSpanIndexInTrace = trace.getAttributes()
        .getAttributeMap().get("head.span.event.index.in.trace").getValue();
    assertEquals("0", actualHeadSpanIndexInTrace);
  }

  @Test
  void headSpanIndexInTracePlacedAtIndexTwoIsAddedToTraceAttributeWithValueTwo() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 2
    Event aExitEvent = createExitEventName("aExitEvent"); // 3
    Event bEntryEvent = createEntryEventWithName("bEvent"); // 4

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{yEntryEvent, zEntryEvent, aEntryHeadSpanEvent, aExitEvent, bEntryEvent},
            new HashMap<>() {
              {
                put(0, new int[]{1});
                put(1, new int[]{2});
                put(2, new int[]{3});
                put(3, new int[]{4});
              }
            });

    new ApiTraceGraph(trace);
    String actualHeadSpanIndexInTrace = trace.getAttributes()
        .getAttributeMap().get("head.span.event.index.in.trace").getValue();
    assertEquals("2", actualHeadSpanIndexInTrace);
  }

  @Test
  void headSpanIndexInTraceNotAddedToTraceAttributeIfNoApiNodesInTrace() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{yEntryEvent, zEntryEvent},
            new HashMap<>() {
              {
                put(0, new int[]{1});
              }
            });

    new ApiTraceGraph(trace);
    assertNull(trace.getAttributes()
        .getAttributeMap().get("head.span.event.index.in.trace"));
  }

  @Test
  void totalNumberOfCallsIsAvailableInHeadSpanAttribute() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 2
    Event aExitEvent = createExitEventName("aExitEvent"); // 3
    Event bEntryEvent = createEntryEventWithName("bEvent"); // 4

    Event[] allEvents = new Event[]{yEntryEvent, zEntryEvent, aEntryHeadSpanEvent, aExitEvent,
        bEntryEvent};
    HashMap<Integer, int[]> eventEdges = new HashMap<>() {
      {
        put(0, new int[]{1});
        put(1, new int[]{2});
        put(2, new int[]{3});
        put(3, new int[]{4});
      }
    };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    String expectedTotalNumberOfCalls = String.valueOf(eventEdges.size());
    String actualTotalNumberOfCalls = apiTraceGraph.getApiNodeList().get(0).getHeadEvent()
        .getEnrichedAttributes().getAttributeMap().get("total.number.of.trace.calls").getValue();

    assertEquals(expectedTotalNumberOfCalls, actualTotalNumberOfCalls);
  }

  @Test
  void totalNumberOfCallsAttributeNotAddedIfThereIsOnlyOneEvent() {
    Event aEntryEvent = createUnspecifiedTypeEventWithName("aEvent"); // 0

    Event[] allEvents = new Event[]{aEntryEvent};

    StructuredTrace trace =
        createTraceWithEventsAndEdges(allEvents, Collections.emptyMap());

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    assertTrue(apiTraceGraph.getApiNodeList().isEmpty());
  }

  @Test
  void totalNumberOfCallsAttributeNotAddedIfThereEventsButNoApiNodes() {
    Event aEntryEvent = createUnspecifiedTypeEventWithName("aEvent"); // 0
    Event bEntryEvent = createUnspecifiedTypeEventWithName("bEvent"); // 1

    Event[] allEvents = new Event[]{aEntryEvent, bEntryEvent};
    HashMap<Integer, int[]> eventEdges = new HashMap<>() {
      {
        put(0, new int[]{1});
      }
    };

    StructuredTrace trace =
        createTraceWithEventsAndEdges(allEvents, eventEdges);

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    assertTrue(apiTraceGraph.getApiNodeList().isEmpty());
  }

  @Test
  void totalNumberOfUniqueApiNodeIsAvailableInHeadSpanAttribute() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 2
    Event aExitEvent = createExitEventName("aExitEvent"); // 3
    Event bEntryEvent = createEntryEventWithName("bEvent"); // 4

    Event[] allEvents = new Event[]{yEntryEvent, zEntryEvent, aEntryHeadSpanEvent, aExitEvent,
        bEntryEvent};
    HashMap<Integer, int[]> eventEdges = new HashMap<>() {
      {
        put(0, new int[]{1});
        put(1, new int[]{2});
        put(2, new int[]{3});
        put(3, new int[]{4});
      }
    };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    String expectedTotalNumberOfUniqueApiNodes = String
        .valueOf(apiTraceGraph.getApiNodeList().size());
    String actualTotalNumberOfCalls = apiTraceGraph.getApiNodeList().get(0).getHeadEvent()
        .getEnrichedAttributes().getAttributeMap().get("total.number.of.unique.trace.api.nodes")
        .getValue();

    assertEquals(expectedTotalNumberOfUniqueApiNodes, actualTotalNumberOfCalls);
  }

  @Test
  void totalNumberOfUniqueApiNodeAttributeNotAddedIfNoApiNodes() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1

    Event[] allEvents = new Event[]{yEntryEvent, zEntryEvent};
    HashMap<Integer, int[]> eventEdges = new HashMap<>() {
      {
        put(0, new int[]{1});
      }
    };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    // we won't be able to get head span to check attribute presence if nodes list is empty, hence
    // this assertion
    assertTrue(apiTraceGraph.getApiNodeList().isEmpty());
  }

  @Test
  void totalNumberOfUniqueApiNodeAttributeEqualsToOneIfAtLeastOneNodeExist() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 2
    Event[] allEvents = new Event[]{yEntryEvent, zEntryEvent, aEntryHeadSpanEvent};
    HashMap<Integer, int[]> eventEdges = new HashMap<>() {
      {
        put(0, new int[]{1});
        put(1, new int[]{2});
      }
    };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    String actualTotalNumberOfCalls = apiTraceGraph.getApiNodeList().get(0).getHeadEvent()
        .getEnrichedAttributes().getAttributeMap().get("total.number.of.unique.trace.api.nodes")
        .getValue();

    assertEquals("1", actualTotalNumberOfCalls);
  }

  private Event createEntryEventWithName(String eventName) {
    Event event = createEntryEvent();
    event.setEventName(eventName);
    return event;
  }

  private Event createExitEventName(String eventName) {
    Event event = createExitEvent();
    event.setEventName(eventName);
    return event;
  }

  private Event createUnspecifiedTypeEventWithName(String eventName) {
    Event event = createUnspecifiedTypeEvent();
    event.setEventName(eventName);
    return event;
  }

  private StructuredTrace createTraceWithEventsAndEdges(
      Event[] events, Map<Integer, int[]> adjList) {
    StructuredTrace trace = createStructuredTrace(events);
    List<Edge> eventEdgeList = new ArrayList<>();

    adjList.forEach(
        (src, list) -> {
          for (int target : list) {
            eventEdgeList.add(
                Edge.newBuilder()
                    .setSrcIndex(src)
                    .setTgtIndex(target)
                    .setEdgeType(EdgeType.EVENT_EVENT)
                    .build());
          }
        });
    trace.setEventEdgeList(eventEdgeList);
    return trace;
  }

  private StructuredTrace createStructuredTrace(Event... events) {
    return createStructuredTraceWithEndTime(System.currentTimeMillis(), events);
  }

  private StructuredTrace createStructuredTraceWithEndTime(
      long endTimeMillis, Event... events) {
    return StructuredTrace.newBuilder()
        .setCustomerId(TEST_CUSTOMER_ID)
        .setTraceId(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()))
        .setStartTimeMillis(endTimeMillis - 10000)
        .setEndTimeMillis(endTimeMillis)
        .setAttributes(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build())
        .setEntityList(new ArrayList<>())
        .setEntityEdgeList(new ArrayList<>())
        .setEventEdgeList(new ArrayList<>())
        .setEntityEventEdgeList(new ArrayList<>())
        .setEventList(Lists.newArrayList(events))
        .build();
  }

  Event createEntryEvent() {
    return createEventOfBoundaryType(
        BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY);
  }

  private Event createEventOfBoundaryType(
      BoundaryTypeValue boundaryTypeValue) {
    Event event = createEvent();
    addEnrichedSpanAttribute(
        event,
        EnrichedSpanConstants.getValue(Api.API_BOUNDARY_TYPE),
        EnrichedSpanConstants.getValue(boundaryTypeValue));
    return event;
  }

  private Event createEvent() {
    return Event.newBuilder()
        .setCustomerId(TEST_CUSTOMER_ID)
        .setEventId(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()))
        .setAttributesBuilder(Attributes.newBuilder().setAttributeMap(new HashMap<>()))
        .setEnrichedAttributesBuilder(Attributes.newBuilder().setAttributeMap(new HashMap<>()))
        .build();
  }

  private void addEnrichedSpanAttribute(
      Event event, String attributeKey, String attributeValue) {
    event
        .getEnrichedAttributes()
        .getAttributeMap()
        .put(attributeKey, AttributeValueCreator.create(attributeValue));
  }

  private Event createExitEvent() {
    return createEventOfBoundaryType(
        BoundaryTypeValue.BOUNDARY_TYPE_VALUE_EXIT);
  }

  private Event createUnspecifiedTypeEvent() {
    return createEventOfBoundaryType(
        BoundaryTypeValue.BOUNDARY_TYPE_VALUE_UNSPECIFIED);
  }
}
