package org.hypertrace.traceenricher.enrichment.enrichers;

import com.google.common.collect.Lists;
import org.hypertrace.attribute.GrpcTagResolver;
import org.hypertrace.attribute.HttpTagResolver;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.attribute.GrpcCodeMapper;
import org.hypertrace.attribute.HttpCodeMapper;

import java.util.List;

/**
 * Enriches Api Status and Status Message
 * Api Status based on status code
 * Api Status Message based on the raw data first, and it's not in span as raw data,
 * find it based on code status
 */
public class ApiStatusEnricher extends AbstractTraceEnricher {

  private static final List<String> grpcStatusCodeKeys = GrpcTagResolver.getGrpcStatusCodeKeys();
  private static final List<String> httpStatusCodeKeys = HttpTagResolver.getHttpStatusCodeKeys();

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    String statusCode = getStatusCode(event, protocol);

    String statusMessage = null;
    String status = null;
    if (Protocol.PROTOCOL_GRPC == protocol) {
      status = GrpcCodeMapper.getState(statusCode);
      statusMessage = GrpcTagResolver.getGrpcStatusMessage(event, statusCode);
    } else if (Protocol.PROTOCOL_HTTP == protocol || Protocol.PROTOCOL_HTTPS == protocol) {
      status = HttpCodeMapper.getState(statusCode);

      statusMessage = HttpTagResolver.getHttpStatusMessage(event, statusCode);
    }
    addEnrichedAttributeIfNotNull(event, EnrichedSpanConstants.getValue(Api.API_STATUS_CODE), statusCode);
    addEnrichedAttributeIfNotNull(event, EnrichedSpanConstants.getValue(Api.API_STATUS_MESSAGE), statusMessage);
    addEnrichedAttributeIfNotNull(event, EnrichedSpanConstants.getValue(Api.API_STATUS), status);
  }

  private static String getStatusCode(Event event, Protocol protocol) {
    List<String> statusCodeKeys = Lists.newArrayList();
    if (Protocol.PROTOCOL_GRPC == protocol) {
      if (event.getGrpc() != null && event.getGrpc().getResponse() != null) {
        int statusCode = event.getGrpc().getResponse().getStatusCode();
        // Checking for the default value for status code field
        if (statusCode != -1) {
          return Integer.toString(statusCode);
        }
      }
      statusCodeKeys.addAll(grpcStatusCodeKeys);
    } else if (Protocol.PROTOCOL_HTTP == protocol || Protocol.PROTOCOL_HTTPS == protocol) {
      statusCodeKeys.addAll(httpStatusCodeKeys);
    }

    return SpanAttributeUtils.getFirstAvailableStringAttribute(event, statusCodeKeys);
  }
}
