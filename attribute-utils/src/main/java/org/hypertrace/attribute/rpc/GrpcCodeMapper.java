package org.hypertrace.attribute.rpc;

import io.grpc.Status;
import io.grpc.Status.Code;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ApiStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcCodeMapper {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcCodeMapper.class);

  public static String getMessage(String code) {
    if (code == null) {
      return null;
    }
    Code grpcCode = fromCodeString(code);
    if (grpcCode == null) {
      return null;
    }

    return grpcCode.name();
  }

  public static String getState(String code) {
    if (code == null) {
      return null;
    }
    Code grpcCode = fromCodeString(code);
    if (grpcCode == null) {
      return null;
    }

    if (Code.OK == grpcCode) {
      return EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_SUCCESS);
    }
    return EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_FAIL);
  }

  private static Code fromCodeString(String code) {
    try {
      int grpcStatusCode = Integer.parseInt(code);
      Status grpcStatus = Status.fromCodeValue(grpcStatusCode);
      return grpcStatus.getCode();
    } catch (NumberFormatException nfe) {
      LOG.error("Incorrect format for GRPC status code: {}", code);
      return null;
    }
  }
}
