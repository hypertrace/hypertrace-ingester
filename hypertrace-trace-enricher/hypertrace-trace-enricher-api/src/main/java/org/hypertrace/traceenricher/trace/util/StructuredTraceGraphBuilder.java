package org.hypertrace.traceenricher.trace.util;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructuredTraceGraphBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredTraceGraphBuilder.class);

  private static ThreadLocal<StructuredTraceGraph> cachedGraph = new ThreadLocal<>();
  private static ThreadLocal<StructuredTrace> cachedTrace = new ThreadLocal<>();

  public static StructuredTraceGraph buildGraph(StructuredTrace trace) {
    if (!GraphBuilderUtil.isSameStructuredTrace(cachedTrace.get(), trace)) {
      Instant start = Instant.now();
      StructuredTraceGraph graph = StructuredTraceGraph.createGraph(trace);
      LOG.debug(
          "Time taken in building StructuredTraceGraph:{} for tenantId:{}",
          Duration.between(start, Instant.now()).toMillis(),
          TimeUnit.MILLISECONDS,
          trace.getCustomerId());
      cachedTrace.set(StructuredTrace.newBuilder(trace).build());
      cachedGraph.set(graph);
      return graph;
    }
    return cachedGraph.get();
  }
}
