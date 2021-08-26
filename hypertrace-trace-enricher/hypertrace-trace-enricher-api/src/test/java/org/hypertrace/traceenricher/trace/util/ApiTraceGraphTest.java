package org.hypertrace.traceenricher.trace.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

  /**
   * A->B->D A->C A->E Depth = 3
   */
  @Test
  void traceWithGraphOfThreeLevelsContainsHeadSpanWithDepthAttributeEqualToThree() {
    Event aEntryEvent = createEntryEventWithName("aEntryEvent"); // 0
    Event aExitEvent = createExitEventName("aExitEvent"); // 1
    Event aExitEvent2 = createExitEventName("aExitEvent2"); // 2
    Event aExitEvent3 = createExitEventName("aExitEvent3"); // 3

    Event bEntryEvent = createEntryEventWithName("bEntryEvent"); // 4
    Event bExitEvent = createExitEventName("bExitEvent"); // 5

    Event cEntryEvent = createEntryEventWithName("cEntryEvent"); // 6
    Event dEntryEvent = createEntryEventWithName("dEntryEvent"); // 7
    Event eEntryEvent = createEntryEventWithName("eEntryEvent"); // 8

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{
                aEntryEvent,
                aExitEvent,
                aExitEvent2,
                aExitEvent3,
                bEntryEvent,
                bExitEvent,
                cEntryEvent,
                dEntryEvent,
                eEntryEvent
            },
            new HashMap<>() {
              {
                put(0, new int[]{1, 2, 3});
                put(1, new int[]{4});
                put(2, new int[]{6});
                put(3, new int[]{8});
                put(4, new int[]{5});
                put(5, new int[]{7});
              }
            });
    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getApiNodeList().get(0).getHeadEvent();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("head_span.api_call_graph_depth")
            .getValue();
    assertEquals("3", actualDepth);
  }

  @Test
  void
  traceWithGraphOfThreeLevelsAndDifferentTypesOfEventsContainsHeadSpanWithDepthAttributeEqualToThree() {
    Event aEntryEvent = createEntryEventWithName("aEntryEvent"); // 0
    Event aUnspecifiedEvent =
        createUnspecifiedTypeEventWithName("aEvent"); // 1
    Event aUnspecifiedEvent2 =
        createUnspecifiedTypeEventWithName("aEvent2"); // 2
    Event aExitEvent = createExitEventName("aExitEvent"); // 3
    Event aExitEvent2 = createExitEventName("aExitEvent2"); // 4

    Event bEntryEvent = createEntryEventWithName("bEntryEvent"); // 5
    Event bUnspecifiedEvent =
        createUnspecifiedTypeEventWithName("bEvent"); // 6
    Event bUnspecifiedEvent2 =
        createUnspecifiedTypeEventWithName("bEvent2"); // 7
    Event bExitEvent = createExitEventName("bExitEvent"); // 8

    Event cEntryEvent = createEntryEventWithName("cEntryEvent"); // 9
    Event dEntryEvent = createEntryEventWithName("dEntryEvent"); // 10

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{
                aEntryEvent,
                aUnspecifiedEvent,
                aUnspecifiedEvent2,
                aExitEvent,
                aExitEvent2,
                bEntryEvent,
                bUnspecifiedEvent,
                bUnspecifiedEvent2,
                bExitEvent,
                cEntryEvent,
                dEntryEvent
            },
            new HashMap<>() {
              {
                put(0, new int[]{1, 4});
                put(1, new int[]{2});
                put(2, new int[]{3});
                put(3, new int[]{5});
                put(4, new int[]{9});
                put(5, new int[]{6});
                put(6, new int[]{7});
                put(7, new int[]{8});
                put(8, new int[]{10});
              }
            });

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getApiNodeList().get(0).getHeadEvent();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("head_span.api_call_graph_depth")
            .getValue();
    assertEquals("3", actualDepth);
  }

  /**
   * Calls sequence A->B->C->A->B->A->B Depth = 7
   */
  @Test
  void traceWithMultipleCyclesContainsHeadSpanWithDepthAttributeEqualToSeven() {
    Event aEntryEvent = createEntryEventWithName("aEntryEvent"); // 0
    Event aExitEvent = createExitEventName("aExitEvent"); // 1
    // A->B
    Event bEntryEvent = createEntryEventWithName("bEntryEvent"); // 2
    Event bExitEvent = createExitEventName("bExitEvent"); // 3
    // B->C
    Event cEntryEvent = createEntryEventWithName("cEntryEvent"); // 4
    Event cExitEvent = createExitEventName("cExitEvent"); // 5
    // C->A
    Event aEntryEvent2 = createEntryEventWithName("aEntryEvent2"); // 6
    Event aExitEvent2 = createExitEventName("aExitEvent2"); // 7
    // A->B
    Event bEntryEvent2 = createEntryEventWithName("bEntryEvent2"); // 8
    Event bExitEvent2 = createExitEventName("bExitEvent2"); // 9
    // B->A
    Event aEntryEvent3 = createEntryEventWithName("aEntryEvent3"); // 10
    Event aExitEvent3 = createExitEventName("aExitEvent3"); // 11
    // A->B
    Event bEntryEvent3 = createEntryEventWithName("bEntryEvent3"); // 12

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{
                aEntryEvent,
                aExitEvent,
                bEntryEvent,
                bExitEvent,
                cEntryEvent,
                cExitEvent,
                aEntryEvent2,
                aExitEvent2,
                bEntryEvent2,
                bExitEvent2,
                aEntryEvent3,
                aExitEvent3,
                bEntryEvent3,
            },
            new HashMap<>() {
              {
                put(0, new int[]{1});
                put(1, new int[]{2});
                put(2, new int[]{3});
                put(3, new int[]{4});
                put(4, new int[]{5});
                put(5, new int[]{6});
                put(6, new int[]{7});
                put(7, new int[]{8});
                put(8, new int[]{9});
                put(9, new int[]{10});
                put(10, new int[]{11});
                put(11, new int[]{12});
              }
            });

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getApiNodeList().get(0).getHeadEvent();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("head_span.api_call_graph_depth")
            .getValue();
    assertEquals("7", actualDepth);
  }

  @Test
  void traceWithOneNodeContainsHeadSpanWithDepthAttributeEqualToOne() {
    Event aEntryEvent = createEntryEventWithName("aEntryEvent");

    StructuredTrace trace =
        createTraceWithEventsAndEdges(new Event[]{aEntryEvent}, new HashMap<>());

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getApiNodeList().get(0).getHeadEvent();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("head_span.api_call_graph_depth")
            .getValue();
    assertEquals("1", actualDepth);
  }

  @Test
  void noHeadSpanForTraceWithOneNonApiBoundaryEvent() {
    Event aEvent = createUnspecifiedTypeEventWithName("aEvent");

    StructuredTrace trace =
        createTraceWithEventsAndEdges(new Event[]{aEvent}, new HashMap<>());

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    assertTrue(apiTraceGraph.getApiNodeList().isEmpty());
  }

  @Test
  void traceWithMultipleDisconnectedNodesContainsHeadSpanWillNotContainDepthAttribute() {
    Event aEntryEvent = createEntryEventWithName("aEntryEvent");
    Event bEntryEvent = createEntryEventWithName("bEntryEvent");
    Event cEntryEvent = createEntryEventWithName("cEntryEvent");

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{aEntryEvent, bEntryEvent, cEntryEvent}, new HashMap<>());

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getApiNodeList().get(0).getHeadEvent();

    assertFalse(
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .containsKey("head_span.call_graph_depth"));
  }

  /**
   * Fractured trace A B->C
   */
  @Test
  void fracturedTraceHeadSpanWillNotContainApiCallDepthAttribute() {
    Event aEntryEvent = createEntryEventWithName("aEntryEvent");
    Event bEntryEvent = createEntryEventWithName("bEntryEvent");
    Event bExitEvent = createExitEventName("bExitEvent");
    Event cEntryEvent = createEntryEventWithName("cEntryEvent");

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{aEntryEvent, bEntryEvent, bExitEvent, cEntryEvent},
            new HashMap<>() {
              {
                put(1, new int[]{2});
                put(2, new int[]{3});
              }
            });

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getApiNodeList().get(0).getHeadEvent();
    assertFalse(
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .containsKey("head_span.call_graph_depth"));
  }

  @Test
  void emptyTraceHeadSpanWillNotContainApiCallDepthAttribute() {
    StructuredTrace trace =
        createTraceWithEventsAndEdges(new Event[]{}, new HashMap<>());

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    assertTrue(apiTraceGraph.getApiNodeList().isEmpty());
  }

  @Test
  void noHeadSpanForTraceWithNoApiBoundaryEvents() {
    Event aEvent = createUnspecifiedTypeEventWithName("aEvent"); // 0
    Event bEvent = createUnspecifiedTypeEventWithName("bEvent"); // 1
    Event cEvent = createUnspecifiedTypeEventWithName("cEvent"); // 2
    Event dEvent = createUnspecifiedTypeEventWithName("dEvent"); // 3

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{aEvent, bEvent, cEvent, dEvent},
            new HashMap<>() {
              {
                put(0, new int[]{1});
                put(1, new int[]{2});
                put(2, new int[]{3});
              }
            });

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    assertTrue(apiTraceGraph.getApiNodeList().isEmpty());
  }

  @Test
  void headSpanOfTraceWithOneApiBoundaryEventContainDepthAttributeEqualToOne() {
    Event aEvent = createUnspecifiedTypeEventWithName("aEvent"); // 0
    Event bEvent = createUnspecifiedTypeEventWithName("bEvent"); // 1
    Event cEvent = createUnspecifiedTypeEventWithName("cEvent"); // 2
    Event dEntryEvent = createEntryEventWithName("dEvent"); // 3

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{aEvent, bEvent, cEvent, dEntryEvent},
            new HashMap<>() {
              {
                put(0, new int[]{1});
                put(1, new int[]{2});
                put(2, new int[]{3});
              }
            });

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getApiNodeList().get(0).getHeadEvent();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("head_span.api_call_graph_depth")
            .getValue();
    assertEquals("1", actualDepth);
  }

  @Test
  void
  headSpanOfTraceWithTwoApiNodesWithEdgeBetweenThemContainsDepthAttributeEqualToTwo() {
    Event aEvent = createUnspecifiedTypeEventWithName("aEvent"); // 0
    Event bEvent = createUnspecifiedTypeEventWithName("bEvent"); // 1
    Event cEvent = createUnspecifiedTypeEventWithName("cEvent"); // 2
    Event dEntryEvent = createEntryEventWithName("dEvent"); // 3
    Event dExitEvent = createExitEventName("dExitEvent"); // 4
    Event hEntryEvent = createEntryEventWithName("dEvent"); // 5

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{aEvent, bEvent, cEvent, dEntryEvent, dExitEvent, hEntryEvent},
            new HashMap<>() {
              {
                put(0, new int[]{1});
                put(1, new int[]{2});
                put(2, new int[]{3});
                put(3, new int[]{4});
                put(4, new int[]{5});
              }
            });

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getApiNodeList().get(0).getHeadEvent();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("head_span.api_call_graph_depth")
            .getValue();
    assertEquals("2", actualDepth);
  }

  /*
   * A->B->C->D
   *       C->E
   * E and D are entries at same 1st level and H entry on 2d level, hence depth = 2
   */
  @Test
  void
  headSpanOfTraceWithThreeBoundaryEventsWithStartingEntryEventContainsDepthAttributeEqualToTwo() {
    Event aEvent = createUnspecifiedTypeEventWithName("aEvent"); // 0
    Event bEvent = createUnspecifiedTypeEventWithName("bEvent"); // 1
    Event cEntryEvent = createEntryEventWithName("cEvent"); // 2
    Event cToDExitEvent = createExitEventName("C->D"); // 3
    Event cToIExitEvent = createExitEventName("C->I"); // 4
    Event dEntryEvent = createEntryEventWithName("dEvent"); // 5
    Event iEntryEvent = createEntryEventWithName("iEvent"); // 6

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[]{
                aEvent, bEvent, cEntryEvent, cToDExitEvent, cToIExitEvent, dEntryEvent, iEntryEvent
            },
            new HashMap<>() {
              {
                put(0, new int[]{1});
                put(1, new int[]{2});
                put(2, new int[]{3, 4});
                put(3, new int[]{5});
                put(4, new int[]{6});
              }
            });

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getApiNodeList().get(0).getHeadEvent();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("head_span.api_call_graph_depth")
            .getValue();
    assertEquals("2", actualDepth);
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

  @Test
  void headSpanContainsTraceStartAndEndTimeAttributes() {
    Event event = createEntryEventWithName("aEntryEvent");

    StructuredTrace trace = createStructuredTrace(event);

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getApiNodeList().get(0).getHeadEvent();
    String startTime =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("head_span.trace_start_time_millis")
            .getValue();
    String endTime =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("head_span.trace_end_time_millis")
            .getValue();

    assertEquals(String.valueOf(trace.getStartTimeMillis()), startTime);
    assertEquals(String.valueOf(trace.getEndTimeMillis()), endTime);
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

    Event[] allEvents = new Event[]{aEntryEvent,bEntryEvent};
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
}
