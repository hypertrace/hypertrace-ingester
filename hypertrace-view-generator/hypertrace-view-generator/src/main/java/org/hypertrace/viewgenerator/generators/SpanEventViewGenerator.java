package org.hypertrace.viewgenerator.generators;

import static org.hypertrace.core.datamodel.shared.SpanAttributeUtils.getStringAttribute;

import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ErrorMetrics;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.viewgenerator.api.SpanEventView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpanEventViewGenerator extends BaseViewGenerator<SpanEventView> {

  private static final String ERROR_COUNT_CONSTANT =
      EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT);

  private static final String EXCEPTION_COUNT_CONSTANT =
      EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT);

  @Override
  List<SpanEventView> generateView(
      StructuredTrace structuredTrace,
      Map<String, Entity> entityMap,
      Map<ByteBuffer, Event> eventMap,
      Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      Map<ByteBuffer, ByteBuffer> childToParentEventIds) {
    Map<ByteBuffer, Event> exitSpanToCalleeApiEntrySpanMap =
        createExitSpanToCalleeApiEntrySpanMap(
            structuredTrace.getEventList(),
            childToParentEventIds,
            parentToChildrenEventIds,
            eventMap);

    Map<ByteBuffer, EventInfo> eventInfoMap = buildEventInfoMap(
        parentToChildrenEventIds, childToParentEventIds, eventMap);
    return structuredTrace.getEventList().stream()
        .map(
            event ->
                generateViewBuilder(
                    event,
                    structuredTrace.getTraceId(),
                    eventMap,
                    childToParentEventIds,
                    exitSpanToCalleeApiEntrySpanMap,
                    eventInfoMap)
                    .build())
        .collect(Collectors.toList());
  }

  @Override
  public String getViewName() {
    return SpanEventView.class.getName();
  }

  @Override
  public Schema getSchema() {
    return SpanEventView.getClassSchema();
  }

  @Override
  public Class<SpanEventView> getViewClass() {
    return SpanEventView.class;
  }

  private SpanEventView.Builder generateViewBuilder(
      final Event event,
      final ByteBuffer traceId,
      final Map<ByteBuffer, Event> eventMap,
      final Map<ByteBuffer, ByteBuffer> childToParentEventIds,
      final Map<ByteBuffer, Event> exitSpanToCalleeApiEntrySpanMap,
      final Map<ByteBuffer, EventInfo> eventInfoMap) {

    SpanEventView.Builder builder = SpanEventView.newBuilder();

    builder.setTenantId(event.getCustomerId());
    builder.setSpanId(event.getEventId());
    builder.setEventName(event.getEventName());
    builder.setApiTraceCount(0);
    EventInfo eventInfo = eventInfoMap.get(event.getEventId());

    if (eventInfo != null) {
      builder.setTotalSpanCount(eventInfo.getTotalEventCount());
      if (null != eventInfo.getApiTraceId()) {
        ByteBuffer apiTraceId = eventInfo.getApiTraceId();
        builder.setApiTraceId(apiTraceId);
        // set this count to 1 only if this span is the head of the Api Trace
        if (event.getEventId().equals(apiTraceId)) {
          builder.setApiTraceCount(1);
        }
        builder.setEntryApiId(EnrichedSpanUtils.getApiId(eventMap.get(apiTraceId)));
      }
    }

    // span_type
    String spanType =
        getStringAttribute(
            event, EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE));
    if (spanType != null) {
      builder.setSpanKind(spanType);
    }

    // parent_span_id
    ByteBuffer parentEventId = childToParentEventIds.get(event.getEventId());
    if (parentEventId != null) {
      builder.setParentSpanId(parentEventId);
    }

    // trace_id
    builder.setTraceId(traceId);

    // service_id, service_name
    builder.setServiceId(EnrichedSpanUtils.getServiceId(event));
    builder.setServiceName(EnrichedSpanUtils.getServiceName(event));

    // api_id, api_name, api_discovery_state
    builder.setApiId(EnrichedSpanUtils.getApiId(event));
    builder.setApiName(EnrichedSpanUtils.getApiName(event));
    builder.setApiDiscoveryState(EnrichedSpanUtils.getApiDiscoveryState(event));

    // display entity and span names
    builder.setDisplayEntityName(getDisplayEntityName(event, exitSpanToCalleeApiEntrySpanMap));
    builder.setDisplaySpanName(getDisplaySpanName(event, exitSpanToCalleeApiEntrySpanMap));

    // protocol_name
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    if (protocol == null || protocol == Protocol.PROTOCOL_UNSPECIFIED) {
      /* In the view, we want to replace unknown with empty string instead
       * for better representation in the UI and easier to filter */
      builder.setProtocolName(EMPTY_STRING);
    } else {
      builder.setProtocolName(EnrichedSpanConstants.getValue(protocol));
    }

    builder.setTags(getAttributeMap(event.getAttributes()));

    // request_url
    builder.setRequestUrl(getRequestUrl(event, protocol));

    // status_code
    builder.setStatusCode(EnrichedSpanUtils.getStatusCode(event));
    builder.setStatus(EnrichedSpanUtils.getStatus(event));
    builder.setStatusMessage(EnrichedSpanUtils.getStatusMessage(event));
    // set boundary type with default value as empty string to avoid null value
    builder.setApiBoundaryType(
        getStringAttribute(event, EnrichedSpanConstants.getValue(Api.API_BOUNDARY_TYPE)));

    // start_time_millis, end_time_millis, duration_millis
    builder.setStartTimeMillis(event.getStartTimeMillis());
    builder.setEndTimeMillis(event.getEndTimeMillis());
    builder.setDurationMillis(event.getEndTimeMillis() - event.getStartTimeMillis());

    // error count
    MetricValue errorMetric = event.getMetrics().getMetricMap().get(ERROR_COUNT_CONSTANT);
    if (errorMetric != null && errorMetric.getValue() > 0.0d) {
      builder.setErrorCount((int) errorMetric.getValue().doubleValue());
    }

    MetricValue exceptionMetric = event.getMetrics().getMetricMap().get(EXCEPTION_COUNT_CONSTANT);
    if (exceptionMetric != null && exceptionMetric.getValue() > 0.0d) {
      builder.setExceptionCount((int) exceptionMetric.getValue().doubleValue());
    }

    builder.setSpaceIds(EnrichedSpanUtils.getSpaceIds(event));

    return builder;
  }

  Map<ByteBuffer, Event> createExitSpanToCalleeApiEntrySpanMap(
      List<Event> spans,
      Map<ByteBuffer, ByteBuffer> childToParentEventIds,
      Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      Map<ByteBuffer, Event> idToEvent) {
    Map<ByteBuffer, Event> exitSpanToCalleeApiEntrySpanMap = new HashMap<>();

    spans.stream()
        .filter(EnrichedSpanUtils::isExitApiBoundary) // Only consider Boundary Exit spans
        .forEach(
            span -> {
              Event calleeEntrySpanForExit =
                  getCalleeEntrySpanForExit(span, parentToChildrenEventIds, idToEvent);
              // We want to map each exit span in the ancestral path of the exit api boundary span
              // to the calleeEntrySpanForExit.
              // So we walk back the path until we hit an entry span or there are no more ancestors
              // in the path.
              Event currentSpan = span;
              while (currentSpan != null && !EnrichedSpanUtils.isEntrySpan(currentSpan)) {
                if (EnrichedSpanUtils.isExitSpan(
                    currentSpan)) { // Skip internal spans and just map the exit spans.
                  exitSpanToCalleeApiEntrySpanMap.put(
                      currentSpan.getEventId(), calleeEntrySpanForExit);
                }
                currentSpan =
                    SpanAttributeUtils.getParentSpan(currentSpan, childToParentEventIds, idToEvent);
              }
            });

    return exitSpanToCalleeApiEntrySpanMap;
  }

  private Event getCalleeEntrySpanForExit(
      Event exitSpan,
      Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      Map<ByteBuffer, Event> idToEvent) {
    List<ByteBuffer> children = parentToChildrenEventIds.get(exitSpan.getEventId());
    if (children == null) {
      return null;
    }

    return children.stream()
        .map(idToEvent::get)
        .filter(
            EnrichedSpanUtils
                ::isEntryApiBoundary) // TODO: Should we just check if span is ENTRY span
        .findFirst()
        .orElse(null);
  }

  /**
   * The entity(service or backend) name to be displayed on the UI. For an entry or internal(not
   * entry or exit) span it will be the same as the span's service_name For an exit span: - if the
   * span maps to a callee api entry span in exitSpanToCalleeApiEntrySpanMap callee's service name -
   * otherwise: -- if backend name is set, we return the backend name. -- if the backend name is not
   * set, we return the span's service name
   */
  String getDisplayEntityName(Event span, Map<ByteBuffer, Event> exitSpanToCalleeApiEntrySpanMap) {
    if (!EnrichedSpanUtils.isExitSpan(span)) { // Not an EXIT span(ENTRY or INTERNAL)
      return EnrichedSpanUtils.getServiceName(span);
    }

    // Exit span with a callee API entry
    Event calleeApiEntrySpan = exitSpanToCalleeApiEntrySpanMap.get(span.getEventId());
    if (calleeApiEntrySpan != null) { // Use Callee service name
      return EnrichedSpanUtils.getServiceName(calleeApiEntrySpan);
    }

    // Backend exit span.
    String backendName = EnrichedSpanUtils.getBackendName(span);
    if (backendName != null && !backendName.isEmpty()) { // Backend name if it's not empty
      return backendName;
    }

    // Use the span's service name if for some reason the backend is unknown
    return EnrichedSpanUtils.getServiceName(span);
  }

  /**
   * The span name to be displayed on the UI. For an entry span it will be the same as the span's
   * api_name For an exit span: - if the span maps to a callee api entry span in
   * exitSpanToCalleeApiEntrySpanMap callee's api name - otherwise: -- if the backend path is set we
   * return the backend path -- if the backend path is not set we return the span name For an
   * internal span, it will be the span name.
   */
  String getDisplaySpanName(Event span, Map<ByteBuffer, Event> exitSpanToCalleeApiEntrySpanMap) {
    if (EnrichedSpanUtils.isEntrySpan(span)) {
      return EnrichedSpanUtils.getApiName(span);
    }

    if (EnrichedSpanUtils.isExitSpan(span)) {
      Event calleeApiEntrySpan = exitSpanToCalleeApiEntrySpanMap.get(span.getEventId());
      if (calleeApiEntrySpan != null) { // Use Callee service api name
        return EnrichedSpanUtils.getApiName(calleeApiEntrySpan);
      }

      // Backend exit span.
      String backendPath = EnrichedSpanUtils.getBackendPath(span);
      // Backend path if it's not empty. If it's empty we will return the span event name at the end
      // of the method
      if (backendPath != null && !backendPath.isEmpty()) {
        return backendPath;
      }
    }

    return span.getEventName();
  }

  String getRequestUrl(Event event, Protocol protocol) {
    if (protocol == null) {
      return null;
    }

    switch (protocol) {
      case PROTOCOL_HTTP:
      case PROTOCOL_HTTPS:
        return EnrichedSpanUtils.getFullHttpUrl(event).orElse(
                Optional.ofNullable(event.getHttp()).map(Http::getRequest).map(Request::getPath)
                    .orElse(null));
      case PROTOCOL_GRPC:
        return event.getEventName();
    }
    return null;
  }

  /**
   * For every event compute EventInfo
   */
  Map<ByteBuffer, EventInfo> buildEventInfoMap(
      final Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      final Map<ByteBuffer, ByteBuffer> childToParentId,
      final Map<ByteBuffer, Event> eventMap) {
    Set<ByteBuffer> rootEventIds = new HashSet<>(eventMap.keySet());
    rootEventIds.removeAll(childToParentId.keySet());
    Map<ByteBuffer, EventInfo> eventInfoMap = Maps.newHashMap();
    rootEventIds.forEach(v -> buildEventInfoMapRecursively(
        parentToChildrenEventIds, eventMap,
        eventInfoMap, v, null));
    return eventInfoMap;
  }

  EventInfo buildEventInfoMapRecursively(
      final Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      final Map<ByteBuffer, Event> eventMap,
      Map<ByteBuffer, EventInfo> eventInfoMap,
      ByteBuffer eventId, ByteBuffer lastApiTraceId) {
    ByteBuffer apiTraceId = EnrichedSpanUtils.isEntryApiBoundary(eventMap.get(eventId))
        ? eventId : lastApiTraceId;

    int totalEventCount = 1;
    List<ByteBuffer> childrenIds = parentToChildrenEventIds.get(eventId);
    if (null != childrenIds) {
      for (ByteBuffer childEventId : childrenIds) {
        totalEventCount += buildEventInfoMapRecursively(
            parentToChildrenEventIds, eventMap,
            eventInfoMap, childEventId, apiTraceId).getTotalEventCount();
      }
    }
    EventInfo eventInfo = new EventInfo(totalEventCount, apiTraceId);
    eventInfoMap.put(eventId, eventInfo);
    return eventInfo;
  }

  private static class EventInfo {
    private final int totalEventCount;
    private final ByteBuffer apiTraceId;

    public EventInfo(int totalEventCount, ByteBuffer apiTraceId) {
      this.totalEventCount = totalEventCount;
      this.apiTraceId = apiTraceId;
    }

    public int getTotalEventCount() {
      return totalEventCount;
    }

    public ByteBuffer getApiTraceId() {
      return apiTraceId;
    }
  }
}
