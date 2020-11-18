package org.hypertrace.attribute.rpc;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.grpc.Response;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.CensusResponse;
import org.hypertrace.core.span.constants.v1.Envoy;
import org.hypertrace.core.span.constants.v1.Grpc;

public class GrpcTagResolver {

  private static final String OTEL_RPC_METHOD = "rpc.method";
  private static final String OTHER_GRPC_METHOD = "grpc.method";

  // status code
  private static final String OTEL_GRPC_STATUS_CODE = "rpc.grpc.status_code";
  private static final String[] OTHER_GRPC_STATUS_CODES =
      {
          RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_STATUS_CODE),
          RawSpanConstants.getValue(Grpc.GRPC_STATUS_CODE),
          RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE)
      };


  public static List<String> getTagsForGrpcMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTEL_RPC_METHOD, OTHER_GRPC_METHOD));
  }

  public static List<String> getGrpcStatusMessageKeys() {
    List<String> grpcStatusMessageKeys = new ArrayList<>();
    grpcStatusMessageKeys.add(RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE));
    grpcStatusMessageKeys.add(RawSpanConstants.getValue(Envoy.ENVOY_GRPC_STATUS_MESSAGE));
    return grpcStatusMessageKeys;
  }

  public static List<String> getGrpcStatusCodeKeys() {
    List<String> grpcStatusCodeKeys = new ArrayList<>(Arrays.asList(OTHER_GRPC_STATUS_CODES));
    grpcStatusCodeKeys.add(OTEL_GRPC_STATUS_CODE);
    return grpcStatusCodeKeys;
  }

  public static String getGrpcStatusMessage(Event event, String statusCode) {
    String statusMessage = null;
    if (event.getGrpc() != null && event.getGrpc().getResponse() != null) {
      Response response = event.getGrpc().getResponse();
      if (StringUtils.isNotBlank(response.getStatusMessage())) {
        statusMessage = response.getStatusMessage();
      } else if (StringUtils.isNotBlank(response.getErrorMessage())) {
        statusMessage = response.getErrorMessage();
      }
    } else {
      statusMessage = SpanAttributeUtils.getFirstAvailableStringAttribute(
          event, GrpcTagResolver.getGrpcStatusMessageKeys());
    }

    // if application tracer doesn't send the status message, then, we'll use
    // our default mapping
    if (statusMessage == null) {
      statusMessage = GrpcCodeMapper.getMessage(statusCode);
    }
    return statusMessage;
  }
}
