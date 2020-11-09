package org.hypertrace.traceenricher.enrichment.enrichers;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.eventfields.grpc.Response;
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

import java.util.ArrayList;
import java.util.List;

/**
 * Enriches Api Status and Status Message
 * Api Status based on status code
 * Api Status Message based on the raw data first, and it's not in span as raw data,
 * fina it based on code status
 */
public class ApiStatusEnricher extends AbstractTraceEnricher {
  private static final List<String> grpcStatusCodeKeys = initializeGrpcStatusCodeKeys();
  private static final List<String> httpStatusCodeKeys = initializeHttpStatusCodeKeys();
  private static final List<String> grpcStatusMessageKeys = initializeGrpcStatusMessageKeys();
  private static final String HTTP_RESPONSE_STATUS_MESSAGE_ATTR = RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_MESSAGE);

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    String statusCode = getStatusCode(event, protocol);

    String statusMessage = null;
    String status = null;
    if (Protocol.PROTOCOL_GRPC == protocol) {
      status = GrpcCodeMapper.getState(statusCode);
      statusMessage = getGrpcStatusMessage(event, statusCode);
    } else if (Protocol.PROTOCOL_HTTP == protocol || Protocol.PROTOCOL_HTTPS == protocol) {
      status = HttpCodeMapper.getState(statusCode);

      statusMessage = getHttpStatusMessage(event, statusCode);
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

  private static String getGrpcStatusMessage(Event event, String statusCode) {
    String statusMessage = null;
    if (event.getGrpc() != null && event.getGrpc().getResponse() != null) {
      Response response = event.getGrpc().getResponse();
      if (StringUtils.isNotBlank(response.getStatusMessage())) {
        statusMessage = response.getStatusMessage();
      } else if (StringUtils.isNotBlank(response.getErrorMessage())) {
        statusMessage = response.getErrorMessage();
      }
    } else {
      statusMessage = SpanAttributeUtils.getFirstAvailableStringAttribute(event, grpcStatusMessageKeys);
    }

    // if application tracer doesn't send the status message, then, we'll use
    // our default mapping
    if (statusMessage == null) {
      statusMessage = GrpcCodeMapper.getMessage(statusCode);
    }
    return statusMessage;
  }

  private static String getHttpStatusMessage(Event event, String statusCode) {
    String statusMessage = SpanAttributeUtils.getStringAttribute(
        event,
        HTTP_RESPONSE_STATUS_MESSAGE_ATTR);
    if (statusMessage == null) {
      statusMessage = HttpCodeMapper.getMessage(statusCode);
    }
    return statusMessage;
  }

  private static List<String> initializeGrpcStatusCodeKeys() {
    List<String> grpcStatusCodeKeys = new ArrayList<>();
    grpcStatusCodeKeys.add(RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_STATUS_CODE));
    grpcStatusCodeKeys.add(RawSpanConstants.getValue(Grpc.GRPC_STATUS_CODE));
    grpcStatusCodeKeys.add(RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE));
    return grpcStatusCodeKeys;
  }

  private static List<String> initializeHttpStatusCodeKeys() {
    List<String> httpStatusCodeKeys = new ArrayList<>();
    httpStatusCodeKeys.add(RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE));
    httpStatusCodeKeys.add(RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_CODE));
    return httpStatusCodeKeys;
  }

  private static List<String> initializeGrpcStatusMessageKeys() {
    List<String> grpcStatusMessageKeys = new ArrayList<>();
    grpcStatusMessageKeys.add(RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE));
    grpcStatusMessageKeys.add(RawSpanConstants.getValue(Envoy.ENVOY_GRPC_STATUS_MESSAGE));
    return grpcStatusMessageKeys;
  }
}
