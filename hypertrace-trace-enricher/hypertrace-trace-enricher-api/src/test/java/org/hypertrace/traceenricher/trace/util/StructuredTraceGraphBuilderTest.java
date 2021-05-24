package org.hypertrace.traceenricher.trace.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import org.hypertrace.core.datamodel.Edge;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class StructuredTraceGraphBuilderTest {

  @Test
  void testBuildGraph() {

    Entity entity = mock(Entity.class);
    Event parent = mock(Event.class);
    Event child = mock(Event.class);
    Edge eventEdge = mock(Edge.class);

    StructuredTrace underTestTrace = mock(StructuredTrace.class);
    when(underTestTrace.getCustomerId()).thenReturn("__defaultTenant");
    when(underTestTrace.getTraceId()).thenReturn(ByteBuffer.wrap("2ebbc19b6428510f".getBytes()));
    when(underTestTrace.getEntityList()).thenReturn(List.of(entity));
    when(underTestTrace.getEventList()).thenReturn(List.of(parent, child));
    when(underTestTrace.getEntityEdgeList()).thenReturn(List.of());
    when(underTestTrace.getEntityEventEdgeList()).thenReturn(List.of());
    when(underTestTrace.getEventEdgeList()).thenReturn(List.of(eventEdge));

    try (MockedStatic<StructuredTrace> builderMockedStatic = mockStatic(StructuredTrace.class)) {

      StructuredTrace.Builder builder = mock(StructuredTrace.Builder.class);
      when(builder.build()).thenReturn(underTestTrace);

      builderMockedStatic
          .when(() -> StructuredTrace.newBuilder(underTestTrace))
          .thenReturn(builder);

      // make two calls, and check that first call create cache entries, and second call uses same
      StructuredTraceGraph mockedStructuredTraceGraph = mock(StructuredTraceGraph.class);
      try (MockedStatic<StructuredTraceGraph> structuredTraceGraphMockedStatic =
          mockStatic(StructuredTraceGraph.class)) {
        structuredTraceGraphMockedStatic
            .when(() -> StructuredTraceGraph.createGraph(underTestTrace))
            .thenReturn(mockedStructuredTraceGraph);

        // calls first time
        StructuredTraceGraph actual = StructuredTraceGraphBuilder.buildGraph(underTestTrace);
        Assertions.assertEquals(mockedStructuredTraceGraph, actual);

        // calls second time, this time it returns from cache
        StructuredTraceGraphBuilder.buildGraph(underTestTrace);
        structuredTraceGraphMockedStatic.verify(
            () -> StructuredTraceGraph.createGraph(underTestTrace), times(1));
      }
    }
  }
}
