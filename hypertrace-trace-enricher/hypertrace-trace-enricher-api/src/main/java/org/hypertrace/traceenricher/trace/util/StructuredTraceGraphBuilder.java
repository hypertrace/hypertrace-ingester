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
    StructuredTraceGraph cachedGraph = cachedGraphThreadLocal.get();
    boolean shouldRebuildTraceEventsGraph =
        GraphBuilderUtil.isTraceEventsChanged(cachedTrace, trace);
    boolean shouldRebuildTraceEntitiesGraph =
        GraphBuilderUtil.isTraceEntitiesChanged(cachedTrace, trace);

    if (null == cachedGraph
        || GraphBuilderUtil.isDifferentTrace(cachedTrace, trace)
        || (shouldRebuildTraceEventsGraph && shouldRebuildTraceEntitiesGraph)) {
      Instant start = Instant.now();
      StructuredTraceGraph graph = new StructuredTraceGraph(trace);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Time taken in building StructuredTraceGraph, duration_millis:{} for tenantId:{}",
            Duration.between(start, Instant.now()).toMillis(),
            trace.getCustomerId());
      }
      cachedTraceThreadLocal.set(StructuredTrace.newBuilder(trace).build());
      cachedGraphThreadLocal.set(graph);
      debugGraph("Case: Rebuilding the graph.", graph, trace);
      return graph;
    }

    if (shouldRebuildTraceEventsGraph || shouldRebuildTraceEntitiesGraph) {
      Instant start = Instant.now();
      if (shouldRebuildTraceEventsGraph) {
        cachedGraph.reCreateTraceEventsGraph(trace);
      } else {
        cachedGraph.reCreateTraceEntitiesGraph(trace);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Time taken in building TraceEventsGraph, duration_millis:{} for tenantId:{}",
            Duration.between(start, Instant.now()).toMillis(),
            trace.getCustomerId());
      }
      cachedTraceThreadLocal.set(StructuredTrace.newBuilder(trace).build());
      cachedGraphThreadLocal.set(cachedGraph);
      debugGraph("Case: Partially building the graph.", cachedGraph, trace);
      return cachedGraph;
    }

    debugGraph("Case: Not building the graph.", cachedGraphThreadLocal.get(), trace);
    return cachedGraph;
  }

  private static void debugGraph(
      String logPrefix, StructuredTraceGraph graph, StructuredTrace trace) {
    if (null != graph
        && (null == graph.getTraceEntitiesGraph() || null == graph.getTraceEventsGraph())) {
      LOG.info(
          logPrefix
              + "StructuredTraceGraph is not built correctly, trace {}, Is events graph non-null: {}."
              + " Is entities graph non-null: {}",
          trace,
          (null != graph.getTraceEventsGraph()),
          (null != graph.getTraceEntitiesGraph()));

      // build the graph again and check
      StructuredTraceGraph tempGraph = new StructuredTraceGraph(trace);
      LOG.info(
          logPrefix
              + "Recreating StructuredTraceGraph. Is events graph non-null: {}."
              + " Is entities graph non-null: {}",
          (null != tempGraph.getTraceEventsGraph()),
          (null != tempGraph.getTraceEntitiesGraph()));

      tempGraph.reCreateTraceEventsGraph(trace);
      LOG.info(
          logPrefix
              + "Recreating events graph. Is events graph non-null: {}."
              + " Is entities graph non-null: {}",
          (null != tempGraph.getTraceEventsGraph()),
          (null != tempGraph.getTraceEntitiesGraph()));

      tempGraph.reCreateTraceEntitiesGraph(trace);
      LOG.info(
          logPrefix
              + "Recreating entities graph. Is events graph non-null: {}."
              + " Is entities graph non-null: {}",
          (null != tempGraph.getTraceEventsGraph()),
          (null != tempGraph.getTraceEntitiesGraph()));
    }
  }
}
