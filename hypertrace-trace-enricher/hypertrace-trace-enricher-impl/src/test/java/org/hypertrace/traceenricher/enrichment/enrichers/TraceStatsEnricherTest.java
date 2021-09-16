package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.traceenricher.enrichment.enrichers.TestUtils.assertTraceEventsDoNotContainAttribute;
import static org.hypertrace.traceenricher.enrichment.enrichers.TestUtils.createEntryEventWithName;
import static org.hypertrace.traceenricher.enrichment.enrichers.TestUtils.createExitEventName;
import static org.hypertrace.traceenricher.enrichment.enrichers.TestUtils.createTraceWithEventsAndEdges;
import static org.hypertrace.traceenricher.enrichment.enrichers.TestUtils.createUnspecifiedTypeEventWithName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.junit.jupiter.api.Test;

class TraceStatsEnricherTest {
  private static final String HEAD_EVENT_ID = "head.event.id";
  private static final String UNIQUE_API_NODES_COUNT = "unique.apis.count";
  private static final String API_ID = "API_ID";

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
  void totalNumberOfUniqueApiNodeIsAvailableInHeadSpanAttribute() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 2
    Event aExitEvent = createExitEventName("aExitEvent"); // 3
    Event bEntryEvent = createEntryEventWithName("bEvent"); // 4

    addApiIdAttr(aEntryHeadSpanEvent, "aIpiId");
    addApiIdAttr(bEntryEvent, "bApiId");

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
            .get(UNIQUE_API_NODES_COUNT)
            .getValue();

    assertEquals("2", actualTotalNumberOfCalls);
  }

  private void addApiIdAttr(Event event, String attributeValue) {
    event
        .getEnrichedAttributes()
        .getAttributeMap()
        .put(API_ID, AttributeValueCreator.create(attributeValue));
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
    assertTraceEventsDoNotContainAttribute(trace, UNIQUE_API_NODES_COUNT);
  }

  @Test
  void totalNumberOfUniqueApiNodeAttributeEqualsToOneIfAtLeastOneNodeExist() {
    Event yEntryEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEntryEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 2
    addApiIdAttr(aEntryHeadSpanEvent, "aApiId");
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
            .get(UNIQUE_API_NODES_COUNT)
            .getValue();

    assertEquals("1", actualTotalNumberOfCalls);
  }

  @Test
  void totalNumberOfUniqueApiNodeAttributeEqualsToTwoEvenThoughFourNodesExist() {
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 0
    addApiIdAttr(aEntryHeadSpanEvent, "aApiId");

    Event aExitEventOne = createExitEventName("aEventOne"); // 1
    Event aExitEvenTwo = createExitEventName("aEventTwo"); // 2
    Event aExitEvenThree = createExitEventName("aEventThree"); // 3

    Event bEntryEventOne = createEntryEventWithName("bEventOne"); // 4
    Event bEntryEventTwo = createEntryEventWithName("bEventTwo"); // 5
    Event bEntryEventThree = createEntryEventWithName("bEventThree"); // 6
    addApiIdAttr(bEntryEventOne, "bApiId");
    addApiIdAttr(bEntryEventTwo, "bApiId");
    addApiIdAttr(bEntryEventThree, "bApiId");

    Event[] allEvents =
        new Event[] {
          aEntryHeadSpanEvent,
          aExitEventOne,
          aExitEvenThree,
          aExitEvenTwo,
          bEntryEventOne,
          bEntryEventTwo,
          bEntryEventThree
        };
    HashMap<Integer, int[]> eventEdges =
        new HashMap<>() {
          {
            put(0, new int[] {1, 2, 3});
            put(1, new int[] {4});
            put(2, new int[] {5});
            put(3, new int[] {6});
          }
        };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    TraceStatsEnricher traceStatsEnricher = new TraceStatsEnricher();
    traceStatsEnricher.enrichTrace(trace);
    String actualTotalNumberOfCalls =
        trace
            .getEventList()
            .get(0)
            .getEnrichedAttributes()
            .getAttributeMap()
            .get(UNIQUE_API_NODES_COUNT)
            .getValue();

    assertEquals("2", actualTotalNumberOfCalls);
  }
}
