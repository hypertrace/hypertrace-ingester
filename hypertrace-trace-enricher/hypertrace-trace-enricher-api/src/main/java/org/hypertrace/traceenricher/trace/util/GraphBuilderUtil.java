package org.hypertrace.traceenricher.trace.util;

import org.hypertrace.core.datamodel.StructuredTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphBuilderUtil {
  public static final Logger LOG = LoggerFactory.getLogger(GraphBuilderUtil.class);

  /**
   * optimistic method of comparing two trace for considering rebuilding of entire graph structure.
   */
  static boolean isSameStructuredTrace(StructuredTrace cachedTrace, StructuredTrace trace) {
    if (cachedTrace == null || trace == null) {
      LOG.debug("Cached and Input trace are not same. Reason: one of the input is null");
      return false;
    }
    // is processed and cached are same trace?
    if (!cachedTrace.getCustomerId().equals(trace.getCustomerId())
        || !cachedTrace.getTraceId().equals(trace.getTraceId())) {
      LOG.debug(
          "Cached and Input trace are not same. Reason: doesn't not match either traceId or tenantId");
      return false;
    }

    // trace internally changed (full trace comparision is costly, so we are doing only with
    // required fields)
    if (cachedTrace.getEntityList().size() != trace.getEntityList().size()
        || cachedTrace.getEventList().size() != trace.getEventList().size()
        || cachedTrace.getEntityEdgeList().size() != trace.getEntityEdgeList().size()
        || cachedTrace.getEntityEventEdgeList().size() != trace.getEntityEventEdgeList().size()
        || cachedTrace.getEventEdgeList().size() != trace.getEventEdgeList().size()) {
      LOG.debug(
          "Cached and Input trace are not same. Reason: they are having different size either for event or entities");
      return false;
    }

    return true;
  }
}
