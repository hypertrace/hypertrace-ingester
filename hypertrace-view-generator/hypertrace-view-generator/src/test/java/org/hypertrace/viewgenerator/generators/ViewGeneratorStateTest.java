package org.hypertrace.viewgenerator.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.viewgenerator.generators.ViewGeneratorState.TraceState;
import org.junit.jupiter.api.Test;

public class ViewGeneratorStateTest {

  ByteBuffer span1 = ByteBuffer.wrap(("span-1".getBytes())), span2 = ByteBuffer.wrap(("span-2".getBytes()));
  String customerId = "customer-1";
  ByteBuffer traceId1 = ByteBuffer.wrap(("trace-1".getBytes())), traceId2 = ByteBuffer.wrap(("trace-2".getBytes()));

  @Test
  public void testTraceState() {
    TraceState traceState = new TraceState(getTestTrace(customerId, traceId1));
    assertEquals(1, traceState.getEntityMap().size());
    assertEquals(2, traceState.getEventMap().size());
    assertEquals(1, traceState.getChildToParentEventIds().size());
    assertEquals(1, traceState.getParentToChildrenEventIds().size());

    assertTrue(traceState.getParentToChildrenEventIds().containsKey(span1));
    assertEquals(1, traceState.getParentToChildrenEventIds().get(span1).size());
    assertTrue(traceState.getChildToParentEventIds().containsKey(span2));
    assertEquals(span1, traceState.getChildToParentEventIds().get(span2));
  }

  @Test
  public void testGetTraceState() {
    StructuredTrace trace = getTestTrace(customerId, traceId1);
    TraceState traceState = ViewGeneratorState.getTraceState(trace);
    assertNotNull(traceState);
    assertEquals(trace, traceState.getTrace());

    StructuredTrace modifiedTrace = getTestTrace(customerId, traceId1);
    modifiedTrace.setEntityList(Arrays.asList(
        Entity.newBuilder()
            .setCustomerId(customerId)
            .setEntityId("entity-2")
            .setEntityName("entity-2")
            .setEntityType("service")
            .build()));

    // same instance should still be returned
    TraceState sameTraceState = ViewGeneratorState.getTraceState(modifiedTrace);
    assertEquals(traceState, sameTraceState);

    StructuredTrace differentTrace = getTestTrace(customerId, traceId2);
    TraceState differentTraceState = ViewGeneratorState.getTraceState(differentTrace);
    assertEquals(differentTrace, differentTraceState.getTrace());
  }

  @Test
  public void testGetApiTraceGraph() {
    StructuredTrace trace = getTestTrace(customerId, traceId1);
    ApiTraceGraph apiTraceGraph = ViewGeneratorState.getApiTraceGraph(trace);
    assertNotNull(apiTraceGraph);

    StructuredTrace modifiedTrace = getTestTrace(customerId, traceId1);
    modifiedTrace.setEntityList(Arrays.asList(
        Entity.newBuilder()
            .setCustomerId(customerId)
            .setEntityId("entity-2")
            .setEntityName("entity-2")
            .setEntityType("service")
            .build()));

    // same instance should still be returned
    ApiTraceGraph sameApiTraceGraph = ViewGeneratorState.getApiTraceGraph(modifiedTrace);
    assertEquals(apiTraceGraph, sameApiTraceGraph);

    StructuredTrace differentTrace = getTestTrace(customerId, traceId2);
    ApiTraceGraph differentApiTraceGraph = ViewGeneratorState.getApiTraceGraph(differentTrace);
    assertNotEquals(apiTraceGraph, differentApiTraceGraph);
  }

  private StructuredTrace getTestTrace(String customerId, ByteBuffer traceId) {
    return StructuredTrace.newBuilder()
        .setCustomerId(customerId)
        .setTraceId(traceId)
        .setStartTimeMillis(20)
        .setEndTimeMillis(30)
        .setEntityList(Arrays.asList(
            Entity.newBuilder()
                .setCustomerId(customerId)
                .setEntityId("entity-1")
                .setEntityName("entity-1")
                .setEntityType("service")
                .build()))
        .setEventList(Arrays.asList(
            Event.newBuilder()
                .setCustomerId(customerId)
                .setEventId(ByteBuffer.wrap(("span-1".getBytes())))
                .setEventName("span-1")
                .build(),
            Event.newBuilder()
                .setCustomerId(customerId)
                .setEventId(ByteBuffer.wrap(("span-2".getBytes())))
                .setEventName("span-2")
                .setEventRefList(Arrays.asList(
                    EventRef.newBuilder()
                        .setTraceId(traceId)
                        .setRefType(EventRefType.CHILD_OF)
                        .setEventId(ByteBuffer.wrap(("span-1".getBytes())))
                        .build()
                ))
                .build()
        ))
        .setEntityEdgeList(Collections.EMPTY_LIST)
        .setEntityEventEdgeList(Collections.EMPTY_LIST)
        .setEventEdgeList(Collections.EMPTY_LIST)
        .setEntityEntityGraph(null)
        .setEventEventGraph(null)
        .setEntityEventGraph(null)
        .setMetrics(null)
        .build();
  }
}
