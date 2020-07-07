package org.hypertrace.traceenricher.enrichment.enrichers;

import com.google.common.collect.Lists;
import java.util.List;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.CensusResponse;
import org.hypertrace.core.span.constants.v1.Envoy;
import org.hypertrace.core.span.constants.v1.Grpc;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.util.GrpcCodeMapper;
import org.hypertrace.traceenricher.util.HttpCodeMapper;

/**
 * Enriches Api Status and Status Message
 * Api Status based on status code
 * Api Status Message based on the raw data first, and it's not in span as raw data,
 * fina it based on code status
 */
public class ApiStatusEnricher extends AbstractTraceEnricher {

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    String statusCode = getStatusCode(event, protocol);
    String statusMessage = null;
    String status = null;
    if (Protocol.PROTOCOL_GRPC == protocol) {
      status = GrpcCodeMapper.getState(statusCode);

      List<String> statusMessageKeys = Lists.newArrayList();
      statusMessageKeys.add(RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE));
      statusMessageKeys.add(RawSpanConstants.getValue(Envoy.ENVOY_GRPC_STATUS_MESSAGE));
      statusMessage = SpanAttributeUtils.getFirstAvailableStringAttribute(event, statusMessageKeys);

      // if application tracer doesn't send the status message, then, we'll use
      // our default mapping
      if (statusMessage == null) {
        statusMessage = GrpcCodeMapper.getMessage(statusCode);
      }
    } else if (Protocol.PROTOCOL_HTTP == protocol || Protocol.PROTOCOL_HTTPS == protocol) {
      status = HttpCodeMapper.getState(statusCode);

      statusMessage = SpanAttributeUtils.getStringAttribute(
          event,
          RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_MESSAGE));
      if (statusMessage == null) {
        statusMessage = HttpCodeMapper.getMessage(statusCode);
      }

    }
    addEnrichedAttributeIfNotNull(event, EnrichedSpanConstants.getValue(Api.API_STATUS_CODE), statusCode);
    addEnrichedAttributeIfNotNull(event, EnrichedSpanConstants.getValue(Api.API_STATUS_MESSAGE), statusMessage);
    addEnrichedAttributeIfNotNull(event, EnrichedSpanConstants.getValue(Api.API_STATUS), status);
  }

  public static String getStatusCode(Event event, Protocol protocol) {
    List<String> statusCodeKeys = Lists.newArrayList();
    if (Protocol.PROTOCOL_GRPC == protocol) {
      statusCodeKeys.add(RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_STATUS_CODE));
      statusCodeKeys.add(RawSpanConstants.getValue(Grpc.GRPC_STATUS_CODE));
      statusCodeKeys.add(RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE));

    } else if (Protocol.PROTOCOL_HTTP == protocol || Protocol.PROTOCOL_HTTPS == protocol) {
      statusCodeKeys.add(RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE));
      statusCodeKeys.add(RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_CODE));
    }

    return SpanAttributeUtils.getFirstAvailableStringAttribute(event, statusCodeKeys);
  }
}
