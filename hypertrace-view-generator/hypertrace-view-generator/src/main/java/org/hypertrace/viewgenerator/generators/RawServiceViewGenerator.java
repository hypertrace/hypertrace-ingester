package org.hypertrace.viewgenerator.generators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ErrorMetrics;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.viewgenerator.api.RawServiceView;

public class RawServiceViewGenerator extends BaseViewGenerator<RawServiceView> {

  @Override
  List<RawServiceView> generateView(
      StructuredTrace structuredTrace,
      Map<String, Entity> entityMap,
      Map<ByteBuffer, Event> eventMap,
      Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      Map<ByteBuffer, ByteBuffer> childToParentEventIds) {
    List<RawServiceView> list = new ArrayList<>();

    // Construct ApiTraceGraph and look at all the head spans within each ApiNode
    ApiTraceGraph apiTraceGraph = ViewGeneratorState.getApiTraceGraph(structuredTrace);

    List<ApiNode<Event>> apiNodes = apiTraceGraph.getNodeList();
    for (ApiNode<Event> apiNode : apiNodes) {
      Event event = apiNode.getHeadEvent();
      if (EnrichedSpanUtils.containsServiceId(event)) {
        RawServiceView.Builder builder = RawServiceView.newBuilder();
        builder.setTenantId(structuredTrace.getCustomerId());
        builder.setTraceId(structuredTrace.getTraceId());
        builder.setSpanId(event.getEventId());
        builder.setParentSpanId(childToParentEventIds.get(event.getEventId()));
        builder.setServiceName(EnrichedSpanUtils.getServiceName(event));
        builder.setServiceId(EnrichedSpanUtils.getServiceId(event));
        builder.setApiName(EnrichedSpanUtils.getApiName(event));
        builder.setApiId(EnrichedSpanUtils.getApiId(event));
        builder.setApiDiscoveryState(EnrichedSpanUtils.getApiDiscoveryState(event));
        builder.setStartTimeMillis(event.getStartTimeMillis());
        builder.setEndTimeMillis(event.getEndTimeMillis());
        builder.setDurationMillis(event.getEndTimeMillis() - event.getStartTimeMillis());
        builder.setTransactionName(getTransactionName(structuredTrace));

        String spanType = EnrichedSpanUtils.getSpanType(event);
        if (spanType != null) {
          builder.setSpanKind(spanType);
        }

        Protocol protocol = EnrichedSpanUtils.getProtocol(event);
        if (protocol == null || protocol == Protocol.PROTOCOL_UNSPECIFIED) {
          /* In the view, we want to replace unknown with empty string instead
           * for better representation in the UI and easier to filter */
          builder.setProtocolName(EMPTY_STRING);
        } else {
          builder.setProtocolName(protocol.name());
        }

        builder.setStatusCode(EnrichedSpanUtils.getStatusCode(event));

        // If this is an API entry boundary span, copy the error count from the event to the view
        // because we want only API or service errors to be present in the view.
        MetricValue errorMetric =
            event
                .getMetrics()
                .getMetricMap()
                .get(EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT));
        if (errorMetric != null && errorMetric.getValue() > 0.0d) {
          builder.setErrorCount((int) errorMetric.getValue().doubleValue());
        }

        // Copy the exception count metric to view directly.
        MetricValue exceptionMetric =
            event
                .getMetrics()
                .getMetricMap()
                .get(EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT));
        if (exceptionMetric != null && exceptionMetric.getValue() > 0.0d) {
          builder.setExceptionCount((int) exceptionMetric.getValue().doubleValue());
        }

        if (EnrichedSpanUtils.isEntrySpan(event)) {
          builder.setNumCalls(1);
        }

        builder.setSpaceIds(EnrichedSpanUtils.getSpaceIds(event));

        list.add(builder.build());
      }
    }
    return list;
  }

  @Override
  public String getViewName() {
    return RawServiceView.class.getName();
  }

  @Override
  public Schema getSchema() {
    return RawServiceView.getClassSchema();
  }

  @Override
  public Class<RawServiceView> getViewClass() {
    return RawServiceView.class;
  }
}
