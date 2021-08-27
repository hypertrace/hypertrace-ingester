package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Edge;
import org.hypertrace.core.datamodel.EdgeType;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.junit.jupiter.api.Test;

class TraceStatsEnricherTest {

  public static final String TEST_CUSTOMER_ID = "testCustomerId";
  public static final String HEAD_EVENT_ID = "head.event.id";
  public static final String TOTAL_NUMBER_OF_UNIQUE_TRACE_API_NODES = "num.unique.apis";
  public static final String TOTAL_NUMBER_OF_TRACE_CALLS = "num.trace.calls";

  @Test
  void headSpanIdIsAddedToTraceAttribute() {
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 0
    Event aExitEvent = createExitEventName("aExitEvent"); // 1
    Event bEntryEvent = createEntryEventWithName("bEvent"); // 2

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[] {aEntryHeadSpanEvent, aExitEvent, bEntryEvent},
            new HashMap<>() {
              {
                put(0, new int[] {1});
                put(1, new int[] {2});
              }
            });

    TraceStatsEnricher traceStatsEnricher = new TraceStatsEnricher();
    traceStatsEnricher.enrichTrace(trace);
    ByteBuffer actualHeadSpanId = aEntryHeadSpanEvent.getEventId();
    assertEquals(
        actualHeadSpanId,
        trace.getAttributes().getAttributeMap().get(HEAD_EVENT_ID).getBinaryValue());
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
            new Event[] {yEntryEvent, zEntryEvent, aEntryHeadSpanEvent, aExitEvent, bEntryEvent},
            new HashMap<>() {
              {
                put(0, new int[] {1});
                put(1, new int[] {2});
                put(2, new int[] {3});
                put(3, new int[] {4});
              }
            });

    TraceStatsEnricher traceStatsEnricher = new TraceStatsEnricher();
    traceStatsEnricher.enrichTrace(trace);
    ByteBuffer expectedHeadSpanId = aEntryHeadSpanEvent.getEventId();
    ByteBuffer actualHeadSpanIndexInTrace =
        trace.getAttributes().getAttributeMap().get(HEAD_EVENT_ID).getBinaryValue();
    assertEquals(expectedHeadSpanId, actualHeadSpanIndexInTrace);
  }

  @Test
  void headSpanIndexInTraceNotAddedToTraceAttributeIfNoApiNodesInTrace() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1

    StructuredTrace trace =
        createTraceWithEventsAndEdges(
            new Event[] {yEntryEvent, zEntryEvent},
            new HashMap<>() {
              {
                put(0, new int[] {1});
              }
            });

    TraceStatsEnricher traceStatsEnricher = new TraceStatsEnricher();
    traceStatsEnricher.enrichTrace(trace);
    assertNull(trace.getAttributes().getAttributeMap().get(HEAD_EVENT_ID));
  }

  @Test
  void totalNumberOfCallsIsAvailableInHeadSpanAttribute() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 2
    Event aExitEvent = createExitEventName("aExitEvent"); // 3
    Event bEntryEvent = createEntryEventWithName("bEvent"); // 4

    Event[] allEvents =
        new Event[] {yEntryEvent, zEntryEvent, aEntryHeadSpanEvent, aExitEvent, bEntryEvent};
    HashMap<Integer, int[]> eventEdges =
        new HashMap<>() {
          {
            put(0, new int[] {1});
            put(1, new int[] {2});
            put(2, new int[] {3});
            put(3, new int[] {4});
          }
        };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    TraceStatsEnricher traceStatsEnricher = new TraceStatsEnricher();
    traceStatsEnricher.enrichTrace(trace);

    String expectedTotalNumberOfCalls = String.valueOf(eventEdges.size());
    String actualTotalNumberOfCalls =
        trace
            .getEventList()
            .get(2)
            .getEnrichedAttributes()
            .getAttributeMap()
            .get(TOTAL_NUMBER_OF_TRACE_CALLS)
            .getValue();

    assertEquals(expectedTotalNumberOfCalls, actualTotalNumberOfCalls);
  }

  @Test
  void totalNumberOfCallsAttributeNotAddedIfThereIsOnlyOneEvent() {
    Event aEntryEvent = createUnspecifiedTypeEventWithName("aEvent"); // 0

    Event[] allEvents = new Event[] {aEntryEvent};

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, Collections.emptyMap());

    TraceStatsEnricher traceStatsEnricher = new TraceStatsEnricher();
    traceStatsEnricher.enrichTrace(trace);

    assertTraceEventsDoNotContainAttribute(trace, TOTAL_NUMBER_OF_TRACE_CALLS);
  }

  @Test
  void totalNumberOfCallsAttributeNotAddedIfThereEventsButNoApiNodes() {
    Event aEntryEvent = createUnspecifiedTypeEventWithName("aEvent"); // 0
    Event bEntryEvent = createUnspecifiedTypeEventWithName("bEvent"); // 1

    Event[] allEvents = new Event[] {aEntryEvent, bEntryEvent};
    HashMap<Integer, int[]> eventEdges =
        new HashMap<>() {
          {
            put(0, new int[] {1});
          }
        };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    TraceStatsEnricher traceStatsEnricher = new TraceStatsEnricher();
    traceStatsEnricher.enrichTrace(trace);
    assertTraceEventsDoNotContainAttribute(trace, TOTAL_NUMBER_OF_TRACE_CALLS);
  }

  private void assertEventDoesNotContainAttribute(Event event, String attributeKey) {
    assertFalse(event.getEnrichedAttributes().getAttributeMap().containsKey(attributeKey));
  }

  private void assertTraceEventsDoNotContainAttribute(StructuredTrace trace, String attributeKey) {
    trace.getEventList().forEach(event -> assertEventDoesNotContainAttribute(event, attributeKey));
  }

  @Test
  void totalNumberOfUniqueApiNodeIsAvailableInHeadSpanAttribute() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 2
    Event aExitEvent = createExitEventName("aExitEvent"); // 3
    Event bEntryEvent = createEntryEventWithName("bEvent"); // 4

    Event[] allEvents =
        new Event[] {yEntryEvent, zEntryEvent, aEntryHeadSpanEvent, aExitEvent, bEntryEvent};
    HashMap<Integer, int[]> eventEdges =
        new HashMap<>() {
          {
            put(0, new int[] {1});
            put(1, new int[] {2});
            put(2, new int[] {3});
            put(3, new int[] {4});
          }
        };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    TraceStatsEnricher traceStatsEnricher = new TraceStatsEnricher();
    traceStatsEnricher.enrichTrace(trace);

    String actualTotalNumberOfCalls =
        trace
            .getEventList()
            .get(2)
            .getEnrichedAttributes()
            .getAttributeMap()
            .get(TOTAL_NUMBER_OF_UNIQUE_TRACE_API_NODES)
            .getValue();

    assertEquals("2", actualTotalNumberOfCalls);
  }

  @Test
  void totalNumberOfUniqueApiNodeAttributeNotAddedIfNoApiNodes() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1

    Event[] allEvents = new Event[] {yEntryEvent, zEntryEvent};
    HashMap<Integer, int[]> eventEdges =
        new HashMap<>() {
          {
            put(0, new int[] {1});
          }
        };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    TraceStatsEnricher traceStatsEnricher = new TraceStatsEnricher();
    traceStatsEnricher.enrichTrace(trace);
    assertTraceEventsDoNotContainAttribute(trace, TOTAL_NUMBER_OF_UNIQUE_TRACE_API_NODES);
  }

  @Test
  void totalNumberOfUniqueApiNodeAttributeEqualsToOneIfAtLeastOneNodeExist() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 2
    Event[] allEvents = new Event[] {yEntryEvent, zEntryEvent, aEntryHeadSpanEvent};
    HashMap<Integer, int[]> eventEdges =
        new HashMap<>() {
          {
            put(0, new int[] {1});
            put(1, new int[] {2});
          }
        };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    TraceStatsEnricher traceStatsEnricher = new TraceStatsEnricher();
    traceStatsEnricher.enrichTrace(trace);
    String actualTotalNumberOfCalls =
        trace
            .getEventList()
            .get(2)
            .getEnrichedAttributes()
            .getAttributeMap()
            .get(TOTAL_NUMBER_OF_UNIQUE_TRACE_API_NODES)
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

  private StructuredTrace createStructuredTraceWithEndTime(long endTimeMillis, Event... events) {
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
    return createEventOfBoundaryType(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY);
  }

  private Event createEventOfBoundaryType(BoundaryTypeValue boundaryTypeValue) {
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

  private void addEnrichedSpanAttribute(Event event, String attributeKey, String attributeValue) {
    event
        .getEnrichedAttributes()
        .getAttributeMap()
        .put(attributeKey, AttributeValueCreator.create(attributeValue));
  }

  private Event createExitEvent() {
    return createEventOfBoundaryType(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_EXIT);
  }

  private Event createUnspecifiedTypeEvent() {
    return createEventOfBoundaryType(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_UNSPECIFIED);
  }
}
