package org.hypertrace.traceenricher.trace.util;

import org.hypertrace.core.datamodel.StructuredTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphBuilderUtil {
  private static final Logger LOG = LoggerFactory.getLogger(GraphBuilderUtil.class);

  /** Check if the new trace is a different trace */
  static boolean isDifferentTrace(StructuredTrace cachedTrace, StructuredTrace trace) {
    if (cachedTrace == null || trace == null) {
      LOG.debug("Cached and Input trace are not same. Reason: one of the input is null");
      return true;
    }
    if (!cachedTrace.getCustomerId().equals(trace.getCustomerId())
        || !cachedTrace.getTraceId().equals(trace.getTraceId())) {
      LOG.debug(
          "Cached and Input trace are not same. Reason: doesn't match either traceId or tenantId");
      return true;
    }
    return false;
  }

  static boolean isStructuredTraceChanged(StructuredTrace cachedTrace, StructuredTrace trace) {
    return isDifferentTrace(cachedTrace, trace) || isTraceEventsChanged(cachedTrace, trace);
  }

  /** Check if the events or theirs edges has changed */
  static boolean isTraceEventsChanged(StructuredTrace cachedTrace, StructuredTrace trace) {
    // trace events internally changed (full trace comparison is costly, so we are doing only with
    // required fields)
    if (isDifferentTrace(cachedTrace, trace)
        || cachedTrace.getEventList().size() != trace.getEventList().size()
        || cachedTrace.getEventEdgeList().size() != trace.getEventEdgeList().size()) {
      LOG.debug(
          "Cached and Input trace are not same. Reason: they are having different size either for event");
      return true;
    }
    return false;
  }
}
