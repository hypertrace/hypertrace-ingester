package org.hypertrace.semantic.convention.utils.rpc;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.semantic.convention.constants.rpc.OTelRpcSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.CensusResponse;
import org.hypertrace.core.span.constants.v1.Grpc;
import org.hypertrace.semantic.convention.utils.span.SpanSemanticConventionUtils;

/**
 * Utility class to fetch rpc attributes
 *
 * <p>The methods in this class might work under the assumption that span data is for specific rpc
 * system
 */
public class RpcSemanticConventionUtils {

  // otel specific attributes
  private static final String OTEL_RPC_SYSTEM = OTelRpcSemanticConventions.RPC_SYSTEM.getValue();
  private static final String OTEL_RPC_METHOD = OTelRpcSemanticConventions.RPC_METHOD.getValue();
  private static final String OTEL_GRPC_STATUS_CODE =
      OTelRpcSemanticConventions.GRPC_STATUS_CODE.getValue();
  private static final String RPC_STATUS_CODE =
      OTelRpcSemanticConventions.RPC_STATUS_CODE.getValue();
  private static final String OTEL_RPC_SYSTEM_GRPC =
      OTelRpcSemanticConventions.RPC_SYSTEM_VALUE_GRPC.getValue();

  private static final String OTHER_GRPC_HOST_PORT = RawSpanConstants.getValue(Grpc.GRPC_HOST_PORT);
  private static final String OTHER_GRPC_METHOD = RawSpanConstants.getValue(Grpc.GRPC_METHOD);
  private static final List<String> ALL_GRPC_STATUS_CODES =
      List.of(
          OTEL_GRPC_STATUS_CODE,
          RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_STATUS_CODE),
          RawSpanConstants.getValue(Grpc.GRPC_STATUS_CODE),
          RPC_STATUS_CODE,
          RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE));

  /** @return attribute keys for grpc method */
  public static List<String> getAttributeKeysForGrpcMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_GRPC_METHOD, OTEL_RPC_METHOD));
  }

  /** @return attribute keys for grpc status code */
  public static List<String> getAttributeKeysForGrpcStatusCode() {
    return ALL_GRPC_STATUS_CODES;
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
   * @param valueMap object encapsulating span data
   * @return if the span is for grpc based on OTel format
   */
  public static boolean isRpcTypeGrpcForOTelFormat(Map<String, AttributeValue> valueMap) {
    return OTEL_RPC_SYSTEM_GRPC.equals(
        valueMap
            .getOrDefault(
                OTEL_RPC_SYSTEM, AttributeValue.newBuilder().setValue(StringUtils.EMPTY).build())
            .getValue());
  }

  /**
   * @param event object encapsulating span data
   * @return uri for grpc span
   */
  public static Optional<String> getGrpcURI(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTHER_GRPC_HOST_PORT)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTHER_GRPC_HOST_PORT));
    } else if (isRpcTypeGrpcForOTelFormat(event)) {
      return SpanSemanticConventionUtils.getURIForOtelFormat(event);
    }
    return Optional.empty();
  }

  /**
   * @param valueMap attribute key value
   * @return uri for grpc span
   */
  public static Optional<String> getGrpcURI(Map<String, AttributeValue> valueMap) {
    if (valueMap.containsKey(OTHER_GRPC_HOST_PORT)) {
      return Optional.of(valueMap.get(OTHER_GRPC_HOST_PORT).getValue());
    } else if (isRpcTypeGrpcForOTelFormat(valueMap)) {
      return SpanSemanticConventionUtils.getURIForOtelFormat(valueMap);
    }
    return Optional.empty();
  }
}
