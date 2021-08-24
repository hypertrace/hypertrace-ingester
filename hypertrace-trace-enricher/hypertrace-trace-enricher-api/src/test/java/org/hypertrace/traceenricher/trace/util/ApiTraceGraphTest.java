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
    String customerId = "testCustomer";

    Event aEntryEvent = createEntryEventWithCustomerAndName(customerId, "aEntryEvent"); // 0
    Event aExitEvent = createExitEventWithCustomerAndName(customerId, "aExitEvent"); // 1
    Event aExitEvent2 = createExitEventWithCustomerAndName(customerId, "aExitEvent2"); // 2
    Event aExitEvent3 = createExitEventWithCustomerAndName(customerId, "aExitEvent3"); // 3

    Event bEntryEvent = createEntryEventWithCustomerAndName(customerId, "bEntryEvent"); // 4
    Event bExitEvent = createExitEventWithCustomerAndName(customerId, "bExitEvent"); // 5

    Event cEntryEvent = createEntryEventWithCustomerAndName(customerId, "cEntryEvent"); // 6
    Event dEntryEvent = createEntryEventWithCustomerAndName(customerId, "dEntryEvent"); // 7
    Event eEntryEvent = createEntryEventWithCustomerAndName(customerId, "eEntryEvent"); // 8

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            customerId,
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

    Event headEvent = apiTraceGraph.getHeadSpan();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("session.api_call_graph_depth")
            .getValue();
    assertEquals("3", actualDepth);
  }

  @Test
  void
  traceWithGraphOfThreeLevelsAndDifferentTypesOfEventsContainsHeadSpanWithDepthAttributeEqualToThree() {
    String customerId = "testCustomer";

    Event aEntryEvent = createEntryEventWithCustomerAndName(customerId, "aEntryEvent"); // 0
    Event aUnspecifiedEvent =
        createUnspecifiedTypeEventWithCustomerAndName(customerId, "aEvent"); // 1
    Event aUnspecifiedEvent2 =
        createUnspecifiedTypeEventWithCustomerAndName(customerId, "aEvent2"); // 2
    Event aExitEvent = createExitEventWithCustomerAndName(customerId, "aExitEvent"); // 3
    Event aExitEvent2 = createExitEventWithCustomerAndName(customerId, "aExitEvent2"); // 4

    Event bEntryEvent = createEntryEventWithCustomerAndName(customerId, "bEntryEvent"); // 5
    Event bUnspecifiedEvent =
        createUnspecifiedTypeEventWithCustomerAndName(customerId, "bEvent"); // 6
    Event bUnspecifiedEvent2 =
        createUnspecifiedTypeEventWithCustomerAndName(customerId, "bEvent2"); // 7
    Event bExitEvent = createExitEventWithCustomerAndName(customerId, "bExitEvent"); // 8

    Event cEntryEvent = createEntryEventWithCustomerAndName(customerId, "cEntryEvent"); // 9
    Event dEntryEvent = createEntryEventWithCustomerAndName(customerId, "dEntryEvent"); // 10

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            customerId,
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

    Event headEvent = apiTraceGraph.getHeadSpan();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("session.api_call_graph_depth")
            .getValue();
    assertEquals("3", actualDepth);
  }

  /**
   * Calls sequence A->B->C->A->B->A->B Depth = 7
   */
  @Test
  void traceWithMultipleCyclesContainsHeadSpanWithDepthAttributeEqualToSeven() {
    String customerId = "testCustomer";

    Event aEntryEvent = createEntryEventWithCustomerAndName(customerId, "aEntryEvent"); // 0
    Event aExitEvent = createExitEventWithCustomerAndName(customerId, "aExitEvent"); // 1
    // A->B
    Event bEntryEvent = createEntryEventWithCustomerAndName(customerId, "bEntryEvent"); // 2
    Event bExitEvent = createExitEventWithCustomerAndName(customerId, "bExitEvent"); // 3
    // B->C
    Event cEntryEvent = createEntryEventWithCustomerAndName(customerId, "cEntryEvent"); // 4
    Event cExitEvent = createExitEventWithCustomerAndName(customerId, "cExitEvent"); // 5
    // C->A
    Event aEntryEvent2 = createEntryEventWithCustomerAndName(customerId, "aEntryEvent2"); // 6
    Event aExitEvent2 = createExitEventWithCustomerAndName(customerId, "aExitEvent2"); // 7
    // A->B
    Event bEntryEvent2 = createEntryEventWithCustomerAndName(customerId, "bEntryEvent2"); // 8
    Event bExitEvent2 = createExitEventWithCustomerAndName(customerId, "bExitEvent2"); // 9
    // B->A
    Event aEntryEvent3 = createEntryEventWithCustomerAndName(customerId, "aEntryEvent3"); // 10
    Event aExitEvent3 = createExitEventWithCustomerAndName(customerId, "aExitEvent3"); // 11
    // A->B
    Event bEntryEvent3 = createEntryEventWithCustomerAndName(customerId, "bEntryEvent3"); // 12

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            customerId,
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

    Event headEvent = apiTraceGraph.getHeadSpan();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("session.api_call_graph_depth")
            .getValue();
    assertEquals("7", actualDepth);
  }

  @Test
  void traceWithOneNodeContainsHeadSpanWithDepthAttributeEqualToOne() {
    String customerId = "testCustomer";
    Event aEntryEvent = createEntryEventWithCustomerAndName(customerId, "aEntryEvent");

    StructuredTrace trace =
        createTraceWithEventsAndEdges(customerId, new Event[]{aEntryEvent}, new HashMap<>());

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getHeadSpan();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("session.api_call_graph_depth")
            .getValue();
    assertEquals("1", actualDepth);
  }

  @Test
  void headSpanForTraceWithOneNonApiBoundaryEventWillBeNull() {
    String customerId = "testCustomer";
    Event aEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "aEvent");

    StructuredTrace trace =
        createTraceWithEventsAndEdges(customerId, new Event[]{aEvent}, new HashMap<>());

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getHeadSpan();
    assertNull(headEvent);
  }

  @Test
  void traceWithMultipleDisconnectedNodesContainsHeadSpanWillNotContainDepthAttribute() {
    String customerId = "testCustomer";
    Event aEntryEvent = createEntryEventWithCustomerAndName(customerId, "aEntryEvent");
    Event bEntryEvent = createEntryEventWithCustomerAndName(customerId, "bEntryEvent");
    Event cEntryEvent = createEntryEventWithCustomerAndName(customerId, "cEntryEvent");

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            customerId, new Event[]{aEntryEvent, bEntryEvent, cEntryEvent}, new HashMap<>());

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getHeadSpan();

    assertFalse(
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .containsKey("session.call_graph_depth"));
  }

  /**
   * Fractured trace A B->C
   */
  @Test
  void fracturedTraceHeadSpanWillNotContainDepthAttribute() {
    String customerId = "testCustomer";
    Event aEntryEvent = createEntryEventWithCustomerAndName(customerId, "aEntryEvent");
    Event bEntryEvent = createEntryEventWithCustomerAndName(customerId, "bEntryEvent");
    Event bExitEvent = createExitEventWithCustomerAndName(customerId, "bExitEvent");
    Event cEntryEvent = createEntryEventWithCustomerAndName(customerId, "cEntryEvent");

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            customerId,
            new Event[]{aEntryEvent, bEntryEvent, bExitEvent, cEntryEvent},
            new HashMap<>() {
              {
                put(1, new int[]{2});
                put(2, new int[]{3});
              }
            });

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getHeadSpan();
    assertFalse(
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .containsKey("session.call_graph_depth"));
  }

  @Test
  void emptyTraceHeadSpanIsNull() {
    String customerId = "testCustomer";
    StructuredTrace trace =
        createTraceWithEventsAndEdges(customerId, new Event[]{}, new HashMap<>());

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getHeadSpan();
    assertNull(headEvent);
  }

  @Test
  void headSpanOfTraceWithNoApiBoundaryEventsWillBeNull() {
    String customerId = "testCustomer";

    Event aEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "aEvent"); // 0
    Event bEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "bEvent"); // 1
    Event cEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "cEvent"); // 2
    Event dEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "dEvent"); // 3

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            customerId,
            new Event[]{aEvent, bEvent, cEvent, dEvent},
            new HashMap<>() {
              {
                put(0, new int[]{1});
                put(1, new int[]{2});
                put(2, new int[]{3});
              }
            });

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getHeadSpan();
    assertNull(headEvent);
  }

  @Test
  void headSpanOfTraceWithOneApiBoundaryEventContainDepthAttributeEqualToOne() {
    String customerId = "testCustomer";

    Event aEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "aEvent"); // 0
    Event bEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "bEvent"); // 1
    Event cEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "cEvent"); // 2
    Event dEntryEvent = createEntryEventWithCustomerAndName(customerId, "dEvent"); // 3

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            customerId,
            new Event[]{aEvent, bEvent, cEvent, dEntryEvent},
            new HashMap<>() {
              {
                put(0, new int[]{1});
                put(1, new int[]{2});
                put(2, new int[]{3});
              }
            });

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getHeadSpan();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("session.api_call_graph_depth")
            .getValue();
    assertEquals("1", actualDepth);
  }

  @Test
  void
  headSpanOfTraceWithTwoApiNodesWithEdgeBetweenThemContainsDepthAttributeEqualToTwo() {
    String customerId = "testCustomer";

    Event aEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "aEvent"); // 0
    Event bEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "bEvent"); // 1
    Event cEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "cEvent"); // 2
    Event dEntryEvent = createEntryEventWithCustomerAndName(customerId, "dEvent"); // 3
    Event dExitEvent = createExitEventWithCustomerAndName(customerId, "dExitEvent"); // 4
    Event hEntryEvent = createEntryEventWithCustomerAndName(customerId, "dEvent"); // 5

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            customerId,
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

    Event headEvent = apiTraceGraph.getHeadSpan();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("session.api_call_graph_depth")
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
    String customerId = "testCustomer";

    Event aEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "aEvent"); // 0
    Event bEvent = createUnspecifiedTypeEventWithCustomerAndName(customerId, "bEvent"); // 1
    Event cEntryEvent = createEntryEventWithCustomerAndName(customerId, "cEvent"); // 2
    Event cToDExitEvent = createExitEventWithCustomerAndName(customerId, "C->D"); // 3
    Event cToIExitEvent = createExitEventWithCustomerAndName(customerId, "C->I"); // 4
    Event dEntryEvent = createEntryEventWithCustomerAndName(customerId, "dEvent"); // 5
    Event iEntryEvent = createEntryEventWithCustomerAndName(customerId, "iEvent"); // 6

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            customerId,
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

    Event headEvent = apiTraceGraph.getHeadSpan();
    String actualDepth =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("session.api_call_graph_depth")
            .getValue();
    assertEquals("2", actualDepth);
  }

  private StructuredTrace createTraceWithEventsAndEdges(
      String customerId, Event[] events, Map<Integer, int[]> adjList) {
    StructuredTrace trace = createStructuredTrace(customerId, events);
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

  private Event createEntryEventWithCustomerAndName(String customerId, String eventName) {
    Event event = createEntryEvent(customerId);
    event.setEventName(eventName);
    return event;
  }

  private Event createExitEventWithCustomerAndName(String customerId, String eventName) {
    Event event = createExitEvent(customerId);
    event.setEventName(eventName);
    return event;
  }

  private Event createUnspecifiedTypeEventWithCustomerAndName(String customerId, String eventName) {
    Event event = createUnspecifiedTypeEvent(customerId);
    event.setEventName(eventName);
    return event;
  }

  @Test
  void headSpanContainsTraceStartAndEndTimeAttributes() {
    String customerId = "testCustomer";
    Event event = createEntryEventWithCustomerAndName(customerId, "aEntryEvent");

    StructuredTrace trace = createStructuredTrace(customerId, event);

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);

    Event headEvent = apiTraceGraph.getHeadSpan();
    String startTime =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("session.trace_start_time_millis")
            .getValue();
    String endTime =
        headEvent
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("session.trace_end_time_millis")
            .getValue();

    assertEquals(String.valueOf(trace.getStartTimeMillis()), startTime);
    assertEquals(String.valueOf(trace.getEndTimeMillis()), endTime);
  }

  private StructuredTrace createStructuredTrace(String customerId, Event... events) {
    return createStructuredTraceWithEndTime(customerId, System.currentTimeMillis(), events);
  }

  private StructuredTrace createStructuredTraceWithEndTime(
      String customerId, long endTimeMillis, Event... events) {
    return StructuredTrace.newBuilder()
        .setCustomerId(customerId)
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

  Event createEntryEvent(String customerId) {
    return createEventOfBoundaryTypeForCustomer(
        BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY, customerId);
  }

  private Event createEventOfBoundaryTypeForCustomer(
      BoundaryTypeValue boundaryTypeValue, String customerId) {
    Event event = createEvent(customerId);
    addEnrichedSpanAttribute(
        event,
        EnrichedSpanConstants.getValue(Api.API_BOUNDARY_TYPE),
        EnrichedSpanConstants.getValue(boundaryTypeValue));
    return event;
  }

  private Event createEvent(String customerId) {
    return Event.newBuilder()
        .setCustomerId(customerId)
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

  private Event createExitEvent(String customerId) {
    return createEventOfBoundaryTypeForCustomer(
        BoundaryTypeValue.BOUNDARY_TYPE_VALUE_EXIT, customerId);
  }

  private Event createUnspecifiedTypeEvent(String customerId) {
    return createEventOfBoundaryTypeForCustomer(
        BoundaryTypeValue.BOUNDARY_TYPE_VALUE_UNSPECIFIED, customerId);
  }
}
