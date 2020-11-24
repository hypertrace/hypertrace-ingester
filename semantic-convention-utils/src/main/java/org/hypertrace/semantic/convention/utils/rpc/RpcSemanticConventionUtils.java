package org.hypertrace.semantic.convention.utils.rpc;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.CensusResponse;
import org.hypertrace.core.span.constants.v1.Grpc;
import org.hypertrace.semantic.convention.utils.span.SpanSemanticConventionUtils;

/**
 * Utility class to fetch rpc attributes
 *
 * The methods in this class might work under the
 * assumption that span data is for specific rpc system
 */
public class RpcSemanticConventionUtils {

  // otel specific attributes
  private static final String OTEL_RPC_SYSTEM = OTelRpcSemanticConventions.RPC_SYSTEM.getValue();
  private static final String OTEL_RPC_METHOD = OTelRpcSemanticConventions.RPC_METHOD.getValue();
  private static final String OTEL_GRPC_STATUS_CODE = OTelRpcSemanticConventions.GRPC_STATUS_CODE.getValue();
  private static final String OTEL_RPC_SYSTEM_GRPC = OTelRpcSemanticConventions.RPC_SYSTEM_VALUE_GRPC.getValue();

  private static final String OTHER_GRPC_HOST_PORT = RawSpanConstants.getValue(Grpc.GRPC_HOST_PORT);
  private static final String OTHER_GRPC_METHOD = RawSpanConstants.getValue(Grpc.GRPC_METHOD);
  private static final String[] OTHER_GRPC_STATUS_CODES =
      {
          RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_STATUS_CODE),
          RawSpanConstants.getValue(Grpc.GRPC_STATUS_CODE),
          RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE)
      };

  /**
   * @return attribute keys for grpc method
   */
  public static List<String> getAttributeKeysForGrpcMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTEL_RPC_METHOD, OTHER_GRPC_METHOD));
  }

  public static List<String> getAttributeKeysForGrpcStatusCode() {

  }

  /**
   * @param event object encapsulating span data
   * @return if the span is for grpc based on OTel format
   */
  public static boolean isRpcTypeGrpcForOTelFormat(Event event) {
    return OTEL_RPC_SYSTEM_GRPC.equals(
        SpanAttributeUtils.getStringAttributeWithDefault(
            event, OTEL_RPC_SYSTEM, StringUtils.EMPTY));
  }

  /**
   * @param event object encapsulating span data
   * @return uri for grpc span
   */
  public static Optional<String> getGrpcURI(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTHER_GRPC_HOST_PORT)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTHER_GRPC_HOST_PORT));
    }
    return SpanSemanticConventionUtils.getURIForOtelFormat(event);
  }
}
