package org.hypertrace.traceenricher.trace.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import org.hypertrace.core.datamodel.Edge;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GraphBuilderUtilTest {

  @Test
  public void testSameTraceForNullInputs() {
    StructuredTrace cachedTrace = mock(StructuredTrace.class);
    StructuredTrace underTestTrace = mock(StructuredTrace.class);

    boolean result = GraphBuilderUtil.isSameStructuredTrace(null, null);
    Assertions.assertFalse(result);

    result = GraphBuilderUtil.isSameStructuredTrace(null, underTestTrace);
    Assertions.assertFalse(result);

    result = GraphBuilderUtil.isSameStructuredTrace(cachedTrace, null);
    Assertions.assertFalse(result);
  }

  @Test
  public void testSameTraceForTenantAndTraceCondition() {
    // different tenant id
    StructuredTrace cachedTrace = mock(StructuredTrace.class);
    when(cachedTrace.getCustomerId()).thenReturn("__defaultTenant");
    when(cachedTrace.getTraceId()).thenReturn(ByteBuffer.wrap("2ebbc19b6428510f".getBytes()));

    StructuredTrace underTestTrace = mock(StructuredTrace.class);
    when(underTestTrace.getCustomerId()).thenReturn("__defaultTenantUnderTest");
    when(underTestTrace.getTraceId()).thenReturn(ByteBuffer.wrap("2ebbc19b6428510f".getBytes()));

    boolean result = GraphBuilderUtil.isSameStructuredTrace(cachedTrace, underTestTrace);
    Assertions.assertFalse(result);

    // different trace ids
    cachedTrace = mock(StructuredTrace.class);
    when(cachedTrace.getCustomerId()).thenReturn("__defaultTenant");
    when(cachedTrace.getTraceId()).thenReturn(ByteBuffer.wrap("2ebbc19b6428510f".getBytes()));

    underTestTrace = mock(StructuredTrace.class);
    when(underTestTrace.getCustomerId()).thenReturn("__defaultTenantUnder");
    when(underTestTrace.getTraceId()).thenReturn(ByteBuffer.wrap("2ebbc19b6428511f".getBytes()));

    result = GraphBuilderUtil.isSameStructuredTrace(cachedTrace, underTestTrace);
    Assertions.assertFalse(result);
  }

  @Test
  public void testSameTraceForSizeCondition() {
    Entity entity = mock(Entity.class);
    Event parent = mock(Event.class);
    Event child = mock(Event.class);
    Edge eventEdge = mock(Edge.class);

    // same size
    StructuredTrace cachedTrace = mock(StructuredTrace.class);
    when(cachedTrace.getCustomerId()).thenReturn("__defaultTenant");
    when(cachedTrace.getTraceId()).thenReturn(ByteBuffer.wrap("2ebbc19b6428510f".getBytes()));
    when(cachedTrace.getEntityList()).thenReturn(List.of(entity));
    when(cachedTrace.getEventList()).thenReturn(List.of(parent, child));
    when(cachedTrace.getEntityEdgeList()).thenReturn(List.of());
    when(cachedTrace.getEntityEventEdgeList()).thenReturn(List.of());
    when(cachedTrace.getEventEdgeList()).thenReturn(List.of(eventEdge));

    StructuredTrace underTestTrace = mock(StructuredTrace.class);
    when(underTestTrace.getCustomerId()).thenReturn("__defaultTenant");
    when(underTestTrace.getTraceId()).thenReturn(ByteBuffer.wrap("2ebbc19b6428510f".getBytes()));
    when(underTestTrace.getEntityList()).thenReturn(List.of(entity));
    when(underTestTrace.getEventList()).thenReturn(List.of(parent, child));
    when(underTestTrace.getEntityEdgeList()).thenReturn(List.of());
    when(underTestTrace.getEntityEventEdgeList()).thenReturn(List.of());
    when(underTestTrace.getEventEdgeList()).thenReturn(List.of(eventEdge));

    boolean result = GraphBuilderUtil.isSameStructuredTrace(cachedTrace, underTestTrace);
    Assertions.assertTrue(result);
  }
}
