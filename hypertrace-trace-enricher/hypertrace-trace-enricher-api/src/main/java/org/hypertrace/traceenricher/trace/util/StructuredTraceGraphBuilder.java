package org.hypertrace.traceenricher.trace.util;

import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructuredTraceGraphBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredTraceGraphBuilder.class);

  private static ThreadLocal<StructuredTraceGraph> cachedGraph = new ThreadLocal<>();
  private static ThreadLocal<StructuredTrace> cachedTrace = new ThreadLocal<>();

  public static StructuredTraceGraph buildGraph(StructuredTrace trace) {
    // trace doesn't exist
    if (cachedTrace.get() == null) {
      LOG.info("Building structured trace graph. Reason: no cached trace");
      StructuredTraceGraph graph = StructuredTraceGraph.createGraph(trace);
      cachedTrace.set(StructuredTrace.newBuilder(trace).build());
      cachedGraph.set(graph);
      return graph;
    }

    // is processed and cached are same trace?
    if (!cachedTrace.get().getCustomerId().equals(trace.getCustomerId()) ||
        !cachedTrace.get().getTraceId().equals(trace.getTraceId())) {
      LOG.info("Building structured trace graph. Reason: cached trace and current trace doesn't not match");
      StructuredTraceGraph graph = StructuredTraceGraph.createGraph(trace);
      cachedTrace.set(StructuredTrace.newBuilder(trace).build());
      cachedGraph.set(graph);
      return graph;
    }

    // trace internally changed
    if(cachedTrace.get().getEntityList().size()  != trace.getEntityList().size() ||
        cachedTrace.get().getEventList().size() != trace.getEventList().size() ||
        cachedTrace.get().getEntityEdgeList().size() != trace.getEntityEdgeList().size() ||
        cachedTrace.get().getEntityEventEdgeList().size() != trace.getEntityEventEdgeList().size() ||
        cachedTrace.get().getEventEdgeList().size() != trace.getEventEdgeList().size()) {
      LOG.info("Building structured trace graph. Reason: cached trace and current trace have different size");
      StructuredTraceGraph graph = StructuredTraceGraph.createGraph(trace);
      cachedTrace.set(StructuredTrace.newBuilder(trace).build());
      cachedGraph.set(graph);
      return graph;
    }

    return cachedGraph.get();
  }
}
