package org.hypertrace.viewgenerator.generators;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ErrorMetrics;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.viewgenerator.api.BackendEntityView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendEntityViewGenerator extends BaseViewGenerator<BackendEntityView> {

  private static final Logger LOG = LoggerFactory.getLogger(BackendEntityViewGenerator.class);
  private static final String BACKEND_HOST_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST);
  private static final String BACKEND_PORT_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT);
  private static final String BACKEND_PATH_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH);
  private static final String BACKEND_PROTOCOL_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL);
  private static final String EXCEPTION_COUNT_ATTR =
      EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT);
  private static final String ERROR_COUNT_ATTR =
      EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT);

  @Override
  List<BackendEntityView> generateView(
      StructuredTrace structuredTrace,
      Map<String, Entity> entityMap,
      Map<ByteBuffer, Event> eventMap,
      Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      Map<ByteBuffer, ByteBuffer> childToParentEventIds) {
    return structuredTrace.getEventList().stream()
        .filter(event -> EnrichedSpanUtils.getBackendId(event) != null)
        .map(
            event ->
                generateViewBuilder(
                    event,
                    structuredTrace,
                    EnrichedSpanUtils.getBackendId(event),
                    entityMap,
                    eventMap,
                    childToParentEventIds)
                    .build())
        .collect(Collectors.toList());
  }

  private BackendEntityView.Builder generateViewBuilder(
      Event event,
      StructuredTrace trace,
      String backendId,
      final Map<String, Entity> entityMap,
      final Map<ByteBuffer, Event> eventMap,
      final Map<ByteBuffer, ByteBuffer> childToParentEventIds) {

    BackendEntityView.Builder builder = BackendEntityView.newBuilder();
    try {
      builder.setTenantId(trace.getCustomerId());
      builder.setTraceId(trace.getTraceId());
      builder.setSpanId(event.getEventId());
      builder.setBackendId(backendId);
      builder.setBackendName(EnrichedSpanUtils.getBackendName(event));
      builder.setBackendHost(
          SpanAttributeUtils.getStringAttributeWithDefault(event, BACKEND_HOST_ATTR, EMPTY_STRING));
      builder.setBackendPort(
          SpanAttributeUtils.getStringAttributeWithDefault(event, BACKEND_PORT_ATTR, EMPTY_STRING));
      builder.setBackendPath(
          SpanAttributeUtils.getStringAttributeWithDefault(event, BACKEND_PATH_ATTR, EMPTY_STRING));
      builder.setBackendProtocol(
          SpanAttributeUtils.getStringAttributeWithDefault(
              event, BACKEND_PROTOCOL_ATTR, EMPTY_STRING));

      builder.setStartTimeMillis(event.getStartTimeMillis());
      builder.setEndTimeMillis(event.getEndTimeMillis());
      builder.setDurationMillis(event.getEndTimeMillis() - event.getStartTimeMillis());
      Protocol protocol = EnrichedSpanUtils.getProtocol(event);

      double exceptionCount = getMetricValue(event, EXCEPTION_COUNT_ATTR, 0.0d);
      builder.setExceptionCount((int) exceptionCount);

      double errorCount = getMetricValue(event, ERROR_COUNT_ATTR, 0.0d);
      builder.setErrorCount((int) errorCount);

      builder.setNumCalls(1);
      String spanType = EnrichedSpanUtils.getSpanType(event);
      if (spanType != null) {
        builder.setSpanKind(spanType);
      }

      builder.setBackendTraceId(HexUtils.getHex(event.getEventId()));

      // caller_service_id
      builder.setCallerServiceId(EnrichedSpanUtils.getServiceId(event));

      // todo: replace this with the start of execution segment entry once the Api Trace is
      // fixed
      // entry_api_id
      Event startApiSpan =
          EnrichedSpanUtils.getApiEntrySpan(event, eventMap, childToParentEventIds);
      if (startApiSpan != null) {
        builder.setCallerApiId(EnrichedSpanUtils.getApiId(startApiSpan));
      }

      builder.setTags(getAttributeMap(event.getAttributes()));

      // this is the same for now
      builder.setDisplayName(event.getEventName());
      // status_code
      builder.setStatusCode(EnrichedSpanUtils.getStatusCode(event));
      builder.setStatus(EnrichedSpanUtils.getStatus(event));
      builder.setStatusMessage(EnrichedSpanUtils.getStatusMessage(event));
      builder.setSpaceIds(EnrichedSpanUtils.getSpaceIds(event));
      builder.setBackendOperationName(EnrichedSpanUtils.getBackendOperationName(event));
    } catch (Exception e) {
      LOG.error(String.format("Failed to generate backendEntityView from Event: %s", event), e);
    }
    return builder;
  }

  @Override
  public String getViewName() {
    return BackendEntityView.class.getName();
  }

  @Override
  public Schema getSchema() {
    return BackendEntityView.getClassSchema();
  }

  @Override
  public Class<BackendEntityView> getViewClass() {
    return BackendEntityView.class;
  }
}
