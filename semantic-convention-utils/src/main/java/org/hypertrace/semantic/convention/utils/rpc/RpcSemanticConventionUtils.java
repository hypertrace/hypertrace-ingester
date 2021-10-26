package org.hypertrace.semantic.convention.utils.rpc;

import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_GRPC_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_BODY_TRUNCATED;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_BODY_TRUNCATED;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_METHOD;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SYSTEM;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_BODY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_BODY_TRUNCATED;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_AUTHORITY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_CONTENT_LENGTH;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_PATH;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_USER_AGENT;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_X_FORWARDED_FOR;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_BODY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_BODY_TRUNCATED;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_METADATA_CONTENT_LENGTH;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.semantic.convention.constants.error.OTelErrorSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.rpc.OTelRpcSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.CensusResponse;
import org.hypertrace.core.span.constants.v1.Envoy;
import org.hypertrace.core.span.constants.v1.Grpc;
import org.hypertrace.core.span.normalizer.constants.OTelRpcSystem;
import org.hypertrace.semantic.convention.utils.span.SpanSemanticConventionUtils;

/**
 * Utility class to fetch rpc attributes
 *
 * <p>The methods in this class might work under the assumption that span data is for specific rpc
 * system
 */
public class RpcSemanticConventionUtils {

  private static final Joiner DOT_JOINER = Joiner.on(".");
  private static final String LOCALHOST = "localhost";
  private static final String COLON = ":";

  // otel specific attributes
  private static final String OTEL_RPC_SYSTEM = OTelRpcSemanticConventions.RPC_SYSTEM.getValue();
  private static final String OTEL_RPC_METHOD = OTelRpcSemanticConventions.RPC_METHOD.getValue();
  private static final String OTEL_GRPC_STATUS_CODE =
      OTelRpcSemanticConventions.GRPC_STATUS_CODE.getValue();
  private static final String RPC_STATUS_CODE =
      OTelRpcSemanticConventions.RPC_STATUS_CODE.getValue();
  private static final String OTEL_RPC_SYSTEM_GRPC =
      OTelRpcSemanticConventions.RPC_SYSTEM_VALUE_GRPC.getValue();
  private static final String OTEL_SPAN_TAG_RPC_SYSTEM_ATTR = OTEL_SPAN_TAG_RPC_SYSTEM.getValue();

  private static final String OTHER_GRPC_HOST_PORT = RawSpanConstants.getValue(Grpc.GRPC_HOST_PORT);
  private static final String OTHER_GRPC_METHOD = RawSpanConstants.getValue(Grpc.GRPC_METHOD);
  private static final List<String> ALL_GRPC_STATUS_CODES =
      List.of(
          OTEL_GRPC_STATUS_CODE,
          RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_STATUS_CODE),
          RawSpanConstants.getValue(Grpc.GRPC_STATUS_CODE),
          RPC_STATUS_CODE,
          RawSpanConstants.getValue(CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE));
  private static final String OTEL_RPC_SERVICE =
      OTelRpcSemanticConventions.RPC_SYSTEM_SERVICE.getValue();

  private static final List<String> STATUS_MSG_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE),
          RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE));

  private static final String GRPC_REQUEST_BODY_TRUNCATED_ATTR =
      RawSpanConstants.getValue(GRPC_REQUEST_BODY_TRUNCATED);
  private static final String RPC_REQUEST_BODY_TRUNCATED_ATTR =
      RPC_REQUEST_BODY_TRUNCATED.getValue();
  private static final String ENVOY_REQUEST_SIZE_ATTR =
      RawSpanConstants.getValue(ENVOY_REQUEST_SIZE);
  private static final String RPC_REQUEST_METADATA_CONTENT_LENGTH_ATTR =
      RPC_REQUEST_METADATA_CONTENT_LENGTH.getValue();
  private static final String GRPC_REQUEST_BODY_ATTR = RawSpanConstants.getValue(GRPC_REQUEST_BODY);
  private static final String RPC_REQUEST_BODY_ATTR = RPC_REQUEST_BODY.getValue();
  private static final String GRPC_RESPONSE_BODY_TRUNCATED_ATTR =
      RawSpanConstants.getValue(GRPC_RESPONSE_BODY_TRUNCATED);
  private static final String RPC_RESPONSE_BODY_TRUNCATED_ATTR =
      RPC_RESPONSE_BODY_TRUNCATED.getValue();
  private static final String ENVOY_RESPONSE_SIZE_ATTR =
      RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE);
  private static final String RPC_RESPONSE_METADATA_CONTENT_LENGTH_ATTR =
      RPC_RESPONSE_METADATA_CONTENT_LENGTH.getValue();
  private static final String GRPC_RESPONSE_BODY_ATTR =
      RawSpanConstants.getValue(GRPC_RESPONSE_BODY);
  private static final String RPC_RESPONSE_BODY_ATTR = RPC_RESPONSE_BODY.getValue();

  /**
   * Differs from {@link
   * org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils#getGrpcURI(Event) in
   * authority resolution logic} }
   *
   * @param event object encapsulating span data
   * @return uri for grpc span
   */
  public static Optional<String> getGrpcURL(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTHER_GRPC_HOST_PORT)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTHER_GRPC_HOST_PORT));
    }
    // look for grpc authority
    Optional<String> grpcAuthority = getSanitizedGrpcAuthority(event);
    if (grpcAuthority.isPresent()) {
      return grpcAuthority;
    }
    if (isRpcTypeGrpcForOTelFormat(event)) {
      return SpanSemanticConventionUtils.getURIForOtelFormat(event);
    }
    return Optional.empty();
  }

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

  public static List<String> getAttributeKeysForRpcService() {
    return Lists.newArrayList(Sets.newHashSet(OTEL_RPC_SERVICE));
  }

  public static Optional<String> getRpcService(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, getAttributeKeysForRpcService()));
  }

  public static Optional<String> getRpcMethod(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, List.of(OTEL_SPAN_TAG_RPC_METHOD.getValue())));
  }

  public static Optional<String> getRpcOperation(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, RpcSemanticConventionUtils.getAttributeKeysForGrpcMethod()));
  }

  public static Boolean isRpcSystemGrpc(Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.get(OTEL_SPAN_TAG_RPC_SYSTEM_ATTR) != null) {
      String val = attributeValueMap.get(OTEL_SPAN_TAG_RPC_SYSTEM_ATTR).getValue();
      return StringUtils.isNotBlank(val)
          && StringUtils.equals(val, OTelRpcSystem.OTEL_RPC_SYSTEM_GRPC.getValue());
    }
    return false;
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

  public static int getGrpcStatusCode(Event event) {
    String grpcStatusCode =
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, RpcSemanticConventionUtils.getAttributeKeysForGrpcStatusCode());
    return grpcStatusCode == null ? -1 : Integer.parseInt(grpcStatusCode);
  }

  public static String getGrpcStatusMsg(Event event) {
    String grpcStatusMsg =
        SpanAttributeUtils.getFirstAvailableStringAttribute(event, STATUS_MSG_ATTRIBUTES);
    return grpcStatusMsg == null ? "" : grpcStatusMsg;
  }

  public static String getGrpcErrorMsg(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return "";
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();

    if (isRpcTypeGrpcForOTelFormat(attributeValueMap)
        && attributeValueMap.get(OTelErrorSemanticConventions.EXCEPTION_MESSAGE.getValue())
            != null) {
      return attributeValueMap
          .get(OTelErrorSemanticConventions.EXCEPTION_MESSAGE.getValue())
          .getValue();
    }

    if (isRpcSystemGrpc(attributeValueMap)
        && attributeValueMap.get(RPC_ERROR_MESSAGE.getValue()) != null) {
      return attributeValueMap.get(RPC_ERROR_MESSAGE.getValue()).getValue();
    }

    if (attributeValueMap.get(RawSpanConstants.getValue(GRPC_ERROR_MESSAGE)) != null) {
      return attributeValueMap.get(RawSpanConstants.getValue(GRPC_ERROR_MESSAGE)).getValue();
    }

    return "";
  }

  public static Optional<String> getGrpcUserAgent(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();

    if (!isRpcSystemGrpc(attributeValueMap)) {
      return Optional.empty();
    }

    if (attributeValueMap.get(RPC_REQUEST_METADATA_USER_AGENT.getValue()) != null
        && !StringUtils.isEmpty(
            attributeValueMap.get(RPC_REQUEST_METADATA_USER_AGENT.getValue()).getValue())) {
      return Optional.of(
          attributeValueMap.get(RPC_REQUEST_METADATA_USER_AGENT.getValue()).getValue());
    }
    return Optional.empty();
  }

  public static Optional<String> getGrpcAuthority(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();

    if (!isRpcSystemGrpc(attributeValueMap)) {
      return Optional.empty();
    }

    if (attributeValueMap.get(RPC_REQUEST_METADATA_AUTHORITY.getValue()) != null) {
      return Optional.of(
          attributeValueMap.get(RPC_REQUEST_METADATA_AUTHORITY.getValue()).getValue());
    }
    return Optional.empty();
  }

  private static boolean isGrpcRequestBodyTruncated(Map<String, AttributeValue> attributeValueMap) {
    Optional<AttributeValue> attributeValue =
        Optional.ofNullable(attributeValueMap.get(GRPC_REQUEST_BODY_TRUNCATED_ATTR));

    return attributeValue.filter(av -> Boolean.parseBoolean(av.getValue())).isPresent();
  }

  private static boolean isRpcRequestBodyTruncated(Map<String, AttributeValue> attributeValueMap) {
    Optional<AttributeValue> attributeValue =
        Optional.ofNullable(attributeValueMap.get(RPC_REQUEST_BODY_TRUNCATED_ATTR));

    return attributeValue.filter(av -> Boolean.parseBoolean(av.getValue())).isPresent();
  }

  public static Optional<Integer> getGrpcRequestSize(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();

    Optional<AttributeValue> attributeValue =
        Optional.ofNullable(attributeValueMap.get(ENVOY_REQUEST_SIZE_ATTR));
    if (attributeValue.isPresent()) {
      return attributeValue.map(av -> Integer.parseInt(av.getValue()));
    }

    attributeValue =
        Optional.ofNullable(attributeValueMap.get(RPC_REQUEST_METADATA_CONTENT_LENGTH_ATTR));
    if (attributeValue.isPresent() && isRpcSystemGrpc(attributeValueMap)) {
      return attributeValue.map(av -> Integer.parseInt(av.getValue()));
    }

    attributeValue = Optional.ofNullable(attributeValueMap.get(GRPC_REQUEST_BODY_ATTR));
    if (attributeValue.isPresent() && !isGrpcRequestBodyTruncated(attributeValueMap)) {
      return attributeValue.map(av -> av.getValue().length());
    }

    attributeValue = Optional.ofNullable(attributeValueMap.get(RPC_REQUEST_BODY_ATTR));
    if (attributeValue.isPresent()
        && isRpcSystemGrpc(attributeValueMap)
        && !isRpcRequestBodyTruncated(attributeValueMap)) {
      return attributeValue.map(av -> av.getValue().length());
    }

    return Optional.empty();
  }

  private static boolean isGrpcResponseBodyTruncated(
      Map<String, AttributeValue> attributeValueMap) {
    Optional<AttributeValue> attributeValue =
        Optional.ofNullable(attributeValueMap.get(GRPC_RESPONSE_BODY_TRUNCATED_ATTR));

    return attributeValue.filter(av -> Boolean.parseBoolean(av.getValue())).isPresent();
  }

  private static boolean isRpcResponseBodyTruncated(Map<String, AttributeValue> attributeValueMap) {
    Optional<AttributeValue> attributeValue =
        Optional.ofNullable(attributeValueMap.get(RPC_RESPONSE_BODY_TRUNCATED_ATTR));

    return attributeValue.filter(av -> Boolean.parseBoolean(av.getValue())).isPresent();
  }

  public static Optional<Integer> getGrpcResponseSize(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    Optional<AttributeValue> attributeValue =
        Optional.ofNullable(attributeValueMap.get(ENVOY_RESPONSE_SIZE_ATTR));
    if (attributeValue.isPresent()) {
      return attributeValue.map(av -> Integer.parseInt(av.getValue()));
    }

    attributeValue =
        Optional.ofNullable(attributeValueMap.get(RPC_RESPONSE_METADATA_CONTENT_LENGTH_ATTR));
    if (attributeValue.isPresent() && isRpcSystemGrpc(attributeValueMap)) {
      return attributeValue.map(av -> Integer.parseInt(av.getValue()));
    }

    attributeValue = Optional.ofNullable(attributeValueMap.get(GRPC_RESPONSE_BODY_ATTR));
    if (attributeValue.isPresent() && !isGrpcResponseBodyTruncated(attributeValueMap)) {
      return attributeValue.map(av -> av.getValue().length());
    }

    attributeValue = Optional.ofNullable(attributeValueMap.get(RPC_RESPONSE_BODY_ATTR));
    if (attributeValue.isPresent()
        && isRpcSystemGrpc(attributeValueMap)
        && !isRpcResponseBodyTruncated(attributeValueMap)) {
      return attributeValue.map(av -> av.getValue().length());
    }

    return Optional.empty();
  }

  public static Optional<String> getGrpcRequestMetadataPath(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();

    if (!isRpcSystemGrpc(attributeValueMap)) {
      return Optional.empty();
    }

    if (attributeValueMap.get(RPC_REQUEST_METADATA_PATH.getValue()) != null) {
      return Optional.of(attributeValueMap.get(RPC_REQUEST_METADATA_PATH.getValue()).getValue());
    }
    return Optional.empty();
  }

  public static Optional<String> getGrpcXForwardedFor(Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return Optional.empty();
    }

    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();

    if (!isRpcSystemGrpc(attributeValueMap)) {
      return Optional.empty();
    }

    if (attributeValueMap.get(RPC_REQUEST_METADATA_X_FORWARDED_FOR.getValue()) != null) {
      return Optional.ofNullable(
          attributeValueMap.get(RPC_REQUEST_METADATA_X_FORWARDED_FOR.getValue()).getValue());
    }
    return Optional.empty();
  }

  public static Optional<String> getRpcPath(Event event) {
    String service = getRpcService(event).orElse("");
    String method = getRpcMethod(event).orElse("");

    if (StringUtils.isNotBlank(service) && StringUtils.isNotBlank(method)) {
      return Optional.of(DOT_JOINER.join(service, method));
    }

    return Optional.empty();
  }

  static Optional<String> getSanitizedGrpcAuthority(Event event) {

    Optional<String> grpcAuthority = getGrpcAuthority(event);
    if (grpcAuthority.isPresent()) {
      return getSanitizedAuthorityValue(grpcAuthority.get());
    } else if (SpanAttributeUtils.containsAttributeKey(
        event, RawSpanConstants.getValue(Envoy.ENVOY_GRPC_AUTHORITY))) {
      return getSanitizedAuthorityValue(
          SpanAttributeUtils.getStringAttribute(
              event, RawSpanConstants.getValue(Envoy.ENVOY_GRPC_AUTHORITY)));
    }
    return Optional.empty();
  }

  static Optional<String> getSanitizedAuthorityValue(String value) {
    if (StringUtils.isBlank(value)) {
      return Optional.empty();
    }
    // authority part of the uri assumes this format: <userinfo@host:port>
    // parsing it as uri doesn't work, since this is not in url format
    // allow for string of type <xyz> or <xyz:port> or <abc@xyz:port>
    List<String> list = Splitter.on(COLON).splitToList(value);
    if (list.size() <= 2 && !value.contains("/")) {
      String host = list.get(0);
      Optional<String> port =
          (list.size() == 2 && !StringUtils.isBlank(list.get(1)))
              ? Optional.of(list.get(1))
              : Optional.empty();
      // reject string of type <:9000> where userinfo@host part is empty
      if (StringUtils.isEmpty(host)) {
        return Optional.empty();
      }
      if (host.contains("@")) {
        List<String> firstHalf = Splitter.on("@").splitToList(host);
        // reject string where host part is empty
        if (firstHalf.size() < 2 || StringUtils.isEmpty(firstHalf.get(1))) {
          return Optional.empty();
        }
        host = firstHalf.get(1);
      }
      if (LOCALHOST.equalsIgnoreCase(host)) {
        return Optional.empty();
      }
      return port.isPresent()
          ? Optional.of(String.format("%s:%s", host, port.get()))
          : Optional.of(host);
    }
    // else string is of type url
    try {
      URI uri = new URI(value);
      if (null != uri.getHost() && !LOCALHOST.equalsIgnoreCase(uri.getHost())) {
        if (-1 != uri.getPort()) {
          return Optional.of(String.format("%s:%s", uri.getHost(), uri.getPort()));
        }
        return Optional.of(uri.getHost());
      }
    } catch (URISyntaxException e) {
      // ignore
    }
    return Optional.empty();
  }
}
