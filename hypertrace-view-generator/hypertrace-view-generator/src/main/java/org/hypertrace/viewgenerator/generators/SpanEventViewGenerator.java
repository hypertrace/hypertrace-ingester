package org.hypertrace.viewgenerator.generators;

import static org.hypertrace.core.datamodel.shared.SpanAttributeUtils.getStringAttribute;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ErrorMetrics;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.viewgenerator.api.SpanEventView;

public class SpanEventViewGenerator extends BaseViewGenerator<SpanEventView> {

  private static final String ERROR_COUNT_CONSTANT =
      EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT);

  private static final String EXCEPTION_COUNT_CONSTANT =
      EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT);

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

  @Override
  List<SpanEventView> generateView(
      StructuredTrace structuredTrace,
      Map<String, Entity> entityMap,
      Map<ByteBuffer, Event> eventMap,
      Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      Map<ByteBuffer, ByteBuffer> childToParentEventIds) {
    Map<ByteBuffer, Event> exitSpanToCalleeApiEntrySpanMap =
        getExitSpanToCalleeApiEntrySpanMap(
            structuredTrace.getEventList(),
            childToParentEventIds,
            parentToChildrenEventIds,
            eventMap);

    return structuredTrace.getEventList().stream()
        .map(
            event ->
                generateViewBuilder(
                        event,
                        structuredTrace.getTraceId(),
                        eventMap,
                        childToParentEventIds,
                        exitSpanToCalleeApiEntrySpanMap)
                    .build())
        .collect(Collectors.toList());
  }

  Map<ByteBuffer, Event> getExitSpanToCalleeApiEntrySpanMap(
      List<Event> spans,
      Map<ByteBuffer, ByteBuffer> childToParentEventIds,
      Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      Map<ByteBuffer, Event> idToEvent) {
    Map<ByteBuffer, Event> exitSpanToCalleeApiEntrySpanMap = new HashMap<>();

    spans.stream()
        .filter(EnrichedSpanUtils::isExitApiBoundary) // Only consider Boundary Exit spans
        .forEach(
            span -> {
              Event apiEntrySpanForExitSpan =
                  getApiEntrySpanForExitSpan(span, parentToChildrenEventIds, idToEvent);
              // We want to map each exit span in the ancestral path of the exit api boundary span
              // to the apiEntrySpanForExitSpan.
              // So we walk back the path until we hit an entry span or there are no more ancestors
              // in the path.
              Event currentSpan = span;
              while (currentSpan != null && !EnrichedSpanUtils.isEntrySpan(currentSpan)) {
                if (EnrichedSpanUtils.isExitSpan(
                    currentSpan)) { // Skip internal spans and just map the exit spans.
                  exitSpanToCalleeApiEntrySpanMap.put(
                      currentSpan.getEventId(), apiEntrySpanForExitSpan);
                }
                currentSpan =
                    SpanAttributeUtils.getParentSpan(currentSpan, childToParentEventIds, idToEvent);
              }
            });

    return exitSpanToCalleeApiEntrySpanMap;
  }

  private Event getApiEntrySpanForExitSpan(
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

  private SpanEventView.Builder generateViewBuilder(
      Event event,
      ByteBuffer traceId,
      Map<ByteBuffer, Event> eventMap,
      Map<ByteBuffer, ByteBuffer> childToParentEventIds,
      Map<ByteBuffer, Event> exitSpanToCalleeApiEntrySpanMap) {

    SpanEventView.Builder builder = SpanEventView.newBuilder();

    builder.setTenantId(event.getCustomerId());
    builder.setSpanId(event.getEventId());
    builder.setEventName(event.getEventName());

    // api_trace_id
    ByteBuffer apiEntrySpanId =
        EnrichedSpanUtils.getApiEntrySpanId(event, eventMap, childToParentEventIds);

    builder.setApiTraceId(apiEntrySpanId);
    if (event.getEventId().equals(apiEntrySpanId)) {
      // set this count to 1 only if this span is the head of the Api Trace
      builder.setApiTraceCount(1);
      if (SpanAttributeUtils.containsAttributeKey(
          event, EnrichedSpanConstants.API_CALLEE_NAME_COUNT_ATTRIBUTE)) {
        builder.setApiCalleeNameCount(
            SpanAttributeUtils.getAttributeValue(
                    event, EnrichedSpanConstants.API_CALLEE_NAME_COUNT_ATTRIBUTE)
                .getValueMap());
      }
    } else {
      builder.setApiTraceCount(0);
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

    // entry_api_id
    Event entryApiSpan = EnrichedSpanUtils.getApiEntrySpan(event, eventMap, childToParentEventIds);
    if (entryApiSpan != null) {
      builder.setEntryApiId(EnrichedSpanUtils.getApiId(entryApiSpan));
    }

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

    builder.setApiExitCalls(
        Integer.parseInt(
            SpanAttributeUtils.getStringAttributeWithDefault(
                event, EnrichedSpanConstants.API_EXIT_CALLS_ATTRIBUTE, "0")));

    builder.setApiTraceErrorSpanCount(
        Integer.parseInt(
            SpanAttributeUtils.getStringAttributeWithDefault(
                event, EnrichedSpanConstants.API_TRACE_ERROR_SPAN_COUNT_ATTRIBUTE, "0")));

    return builder;
  }

  /**
   * The entity(service or backend) name to be displayed on the UI. For an entry or internal(not
   * entry or exit) span it will be the same as the span's service_name. for an exit span: - if the
   * span maps to an api entry span in {@code exitSpanToCalleeApiEntrySpanMap} api entry service
   * name - if backend name is set, we return the backend name - if the backend name is not set, we
   * return the span's service name
   */
  String getDisplayEntityName(Event span, Map<ByteBuffer, Event> exitSpanToCalleeApiEntrySpanMap) {
    if (!EnrichedSpanUtils.isExitSpan(span)) { // Not an EXIT span(ENTRY or INTERNAL)
      return EnrichedSpanUtils.getServiceName(span);
    }

    // Exit span with a callee API entry
    Event apiEntrySpan = exitSpanToCalleeApiEntrySpanMap.get(span.getEventId());
    if (apiEntrySpan != null) { // Use Callee service name
      return EnrichedSpanUtils.getServiceName(apiEntrySpan);
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
   * api_name For an exit span: - if the span maps to a callee api entry span in {@code
   * exitSpanToCalleeApiEntrySpanMap} callee api name - if the backend path is set we return the
   * backend path - if the backend path is not set we return the span name For an internal span, it
   * will be the span name
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
        return EnrichedSpanUtils.getFullHttpUrl(event)
            .orElse(
                Optional.ofNullable(event.getHttp())
                    .map(Http::getRequest)
                    .map(Request::getPath)
                    .orElse(null));
      case PROTOCOL_GRPC:
        return event.getEventName();
    }
    return null;
  }
}
