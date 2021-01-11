package org.hypertrace.viewgenerator.generators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;

public class ViewGeneratorState {

  private static final ThreadLocal<TraceState> traceStateThreadLocal = new ThreadLocal<>();
  private static final ThreadLocal<ApiTraceGraph> apiTraceGraphThreadLocal = new ThreadLocal<>();

  public static ApiTraceGraph getApiTraceGraph(StructuredTrace trace) {
    if (apiTraceGraphThreadLocal.get() == null
        || isDifferentTrace(apiTraceGraphThreadLocal.get().getTrace(), trace)) {
      apiTraceGraphThreadLocal.set(new ApiTraceGraph(trace).build());
    }
    return apiTraceGraphThreadLocal.get();
  }

  public static TraceState getTraceState(StructuredTrace trace) {
    if (traceStateThreadLocal.get() == null
        || isDifferentTrace(traceStateThreadLocal.get().trace, trace)) {
      traceStateThreadLocal.set(new TraceState(trace));
    }
    return traceStateThreadLocal.get();
  }

  private static boolean isDifferentTrace(StructuredTrace cached, StructuredTrace trace) {
    return !cached.getCustomerId().equals(trace.getCustomerId())
        || !cached.getTraceId().equals(trace.getTraceId());
  }

  public static class TraceState {
    private final StructuredTrace trace;
    final Map<String, Entity> entityMap = new HashMap<>();
    final Map<ByteBuffer, Event> eventMap = new HashMap<>();
    final Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds = new HashMap<>();
    final Map<ByteBuffer, ByteBuffer> childToParentEventIds = new HashMap<>();

    public TraceState(StructuredTrace trace) {
      this.trace = trace;
      for (Entity entity : trace.getEntityList()) {
        entityMap.put(entity.getEntityId(), entity);
      }
      for (Event event : trace.getEventList()) {
        eventMap.put(event.getEventId(), event);
      }

      for (Event event : trace.getEventList()) {
        ByteBuffer childEventId = event.getEventId();
        List<EventRef> eventRefs = eventMap.get(childEventId).getEventRefList();
        if (eventRefs != null) {
          eventRefs.stream()
              .filter(eventRef -> EventRefType.CHILD_OF == eventRef.getRefType())
              .forEach(
                  eventRef -> {
                    ByteBuffer parentEventId = eventRef.getEventId();
                    if (!parentToChildrenEventIds.containsKey(parentEventId)) {
                      parentToChildrenEventIds.put(parentEventId, new ArrayList<>());
                    }
                    parentToChildrenEventIds.get(parentEventId).add(childEventId);
                    childToParentEventIds.put(childEventId, parentEventId);
                  });
        }
        // expected only 1 childOf relationship
      }
    }
  }
}
