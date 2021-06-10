package org.hypertrace.traceenricher.trace.util;

import java.time.Duration;
import java.time.Instant;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructuredTraceGraphBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredTraceGraphBuilder.class);

  private static final ThreadLocal<StructuredTraceGraph> cachedGraphThreadLocal =
      new ThreadLocal<>();
  private static final ThreadLocal<StructuredTrace> cachedTraceThreadLocal = new ThreadLocal<>();

  public static StructuredTraceGraph buildGraph(StructuredTrace trace) {
    StructuredTrace cachedTrace = cachedTraceThreadLocal.get();
    boolean shouldRebuildTraceEventsGraph =
        GraphBuilderUtil.isTraceEventsChanged(cachedTrace, trace);
    boolean shouldRebuildTraceEntitiesGraph =
        GraphBuilderUtil.isTraceEntitiesChanged(cachedTrace, trace);

    if (GraphBuilderUtil.isDifferentTrace(cachedTrace, trace)
        || (shouldRebuildTraceEventsGraph && shouldRebuildTraceEntitiesGraph)) {
      Instant start = Instant.now();
      StructuredTraceGraph graph = StructuredTraceGraph.createGraph(trace);
      LOG.debug(
          "Time taken in building StructuredTraceGraph, duration_millis:{} for tenantId:{}",
          Duration.between(start, Instant.now()).toMillis(),
          trace.getCustomerId());
      cachedTraceThreadLocal.set(StructuredTrace.newBuilder(trace).build());
      cachedGraphThreadLocal.set(graph);
      return graph;
    }

    if (shouldRebuildTraceEventsGraph || shouldRebuildTraceEntitiesGraph) {
      Instant start = Instant.now();
      StructuredTraceGraph graph =
          shouldRebuildTraceEventsGraph
              ? StructuredTraceGraph.reCreateTraceEventsGraph(trace)
              : StructuredTraceGraph.reCreateTraceEntitiesGraph(trace);
      LOG.debug(
          "Time taken in building TraceEventsGraph, duration_millis:{} for tenantId:{}",
          Duration.between(start, Instant.now()).toMillis(),
          trace.getCustomerId());
      cachedTraceThreadLocal.set(StructuredTrace.newBuilder(trace).build());
      cachedGraphThreadLocal.set(graph);
      return graph;
    }
    return cachedGraphThreadLocal.get();
  }
}
