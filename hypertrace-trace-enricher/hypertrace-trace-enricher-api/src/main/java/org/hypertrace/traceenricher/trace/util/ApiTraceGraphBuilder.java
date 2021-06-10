package org.hypertrace.traceenricher.trace.util;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiTraceGraphBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(ApiTraceGraphBuilder.class);

  private static ThreadLocal<ApiTraceGraph> cachedGraph = new ThreadLocal<>();
  private static ThreadLocal<StructuredTrace> cachedTrace = new ThreadLocal<>();

  public static ApiTraceGraph buildGraph(StructuredTrace trace) {
    if (!GraphBuilderUtil.isSameStructuredTrace(cachedTrace.get(), trace)) {
      Instant start = Instant.now();
      ApiTraceGraph graph = new ApiTraceGraph(trace);
      LOG.debug(
          "Time taken in building ApiTraceGraph:{} for tenantId:{}",
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
