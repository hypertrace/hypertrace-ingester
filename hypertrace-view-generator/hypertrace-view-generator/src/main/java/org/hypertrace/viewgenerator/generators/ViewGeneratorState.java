package org.hypertrace.viewgenerator.generators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.reflect.Nullable;
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
      apiTraceGraphThreadLocal.set(new ApiTraceGraph(trace));
    }
    return apiTraceGraphThreadLocal.get();
  }

  public static TraceState getTraceState(StructuredTrace trace) {
    if (traceStateThreadLocal.get() == null
        || isDifferentTrace(traceStateThreadLocal.get().getTrace(), trace)) {
      traceStateThreadLocal.set(new TraceState(trace));
    }
    return traceStateThreadLocal.get();
  }

  private static boolean isDifferentTrace(StructuredTrace cached, StructuredTrace trace) {
    return cached != trace;
  }

  public static class TraceState {
    private final StructuredTrace trace;
    private final Map<String, Entity> entityMap = new HashMap<>();
    private final Map<ByteBuffer, Event> eventMap = new HashMap<>();
    private final Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds = new HashMap<>();
    private final Map<ByteBuffer, ByteBuffer> childToParentEventIds = new HashMap<>();

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
        ByteBuffer parentEventId = getParentEventId(event);
        if (parentEventId != null) {
          parentToChildrenEventIds
              .computeIfAbsent(parentEventId, v -> new ArrayList<>())
              .add(childEventId);
          childToParentEventIds.put(childEventId, parentEventId);
        }
      }
    }

    /**
     * Note:
     * As of now, open tracing specification represents two construct for providing support
     * for relation between spans: child_of and follow_from.
     * Ref : https://opentracing.io/specification/
     *
     * On the other hand, as of now, open telemetry supports only concept of parent
     * Ref : https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto#L85
     *
     * However, there are open discussion for similar construct on otel community.
     * Ref: https://github.com/open-telemetry/opentelemetry-specification/issues/65
     *
     * As, both the construct `child_of` and `follow_from` represent parent-child relation in common
     * where in one case parent is interested in child span's result while in other case not.
     *
     * So, to support common behaviour, we will be establish link for `follow_from` as well.
     *
     * Also expecting only 1 child-parent relation
     * */
    @Nullable
    private ByteBuffer buildParentChildLinks(Event event) {
      Map<Boolean, List<EventRef>> referenceSplits =
          event.getEventRefList().stream()
              .collect(
                  Collectors.partitioningBy(
                      eventRef -> eventRef.getRefType() == EventRefType.CHILD_OF));

      List<EventRef> childEventRef = referenceSplits.get(true);
      if (childEventRef != null && childEventRef.size() > 0) {
        return childEventRef.get(0).getEventId();
      } else {
        List<EventRef> followFromEventRef = referenceSplits.get(false);
        if (followFromEventRef != null && followFromEventRef.size() == 1) {
          return followFromEventRef.get(0).getEventId();
        }
      }
      return null;
    }

    public StructuredTrace getTrace() {
      return trace;
    }

    public Map<String, Entity> getEntityMap() {
      return entityMap;
    }

    public Map<ByteBuffer, Event> getEventMap() {
      return eventMap;
    }

    public Map<ByteBuffer, List<ByteBuffer>> getParentToChildrenEventIds() {
      return parentToChildrenEventIds;
    }

    public Map<ByteBuffer, ByteBuffer> getChildToParentEventIds() {
      return childToParentEventIds;
    }
  }
}
