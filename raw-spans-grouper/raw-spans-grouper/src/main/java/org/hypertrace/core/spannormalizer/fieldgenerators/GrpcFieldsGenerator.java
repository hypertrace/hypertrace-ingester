package org.hypertrace.core.spannormalizer.fieldgenerators;

import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_GRPC_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_ERROR_NAME;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_HOST_PORT;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_BODY_TRUNCATED;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_CALL_OPTIONS;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_METADATA;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_BODY_TRUNCATED;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_METADATA;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_ERROR_NAME;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_BODY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_BODY_TRUNCATED;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_AUTHORITY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_CONTENT_LENGTH;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_CONTENT_TYPE;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_PATH;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_USER_AGENT;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_X_FORWARDED_FOR;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_BODY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_BODY_TRUNCATED;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_METADATA_CONTENT_LENGTH;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_METADATA_CONTENT_TYPE;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_STATUS_CODE;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.grpc.Grpc;
import org.hypertrace.core.semantic.convention.constants.error.OTelErrorSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;

public class GrpcFieldsGenerator extends ProtocolFieldsGenerator<Grpc.Builder> {
  private static final String METADATA_STR_VAL_PREFIX = "Metadata(";
  private static final List<String> STATUS_MESSAGE_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE),
          RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE));
  private static final Map<String, String> EMPTY_METADATA_MAP = Map.of();
  private static final String METADATA_DELIMITER = ",";
  private static final String METADATA_KEY_VALUE_DELIMITER = "=";
  private static final Map<String, FieldGenerator<Grpc.Builder>> fieldGeneratorMap =
      initializeFieldGenerators();
  private static final Map<String, FieldGenerator<Grpc.Builder>> rpcFieldGeneratorMap =
      initializeRpcFieldGenerators();

  private static Map<String, FieldGenerator<Grpc.Builder>> initializeFieldGenerators() {
    Map<String, FieldGenerator<Grpc.Builder>> fieldGeneratorMap = new HashMap<>();

    fieldGeneratorMap.put(
        RawSpanConstants.getValue(GRPC_REQUEST_BODY),
        (key, keyValue, builder, tagsMap) -> {
          builder.getRequestBuilder().setBody(ValueConverter.getString(keyValue));
          setRequestSize(builder, tagsMap);
        });
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(GRPC_RESPONSE_BODY),
        (key, keyValue, builder, tagsMap) -> {
          builder.getResponseBuilder().setBody(ValueConverter.getString(keyValue));
          setResponseSize(builder, tagsMap);
        });

    fieldGeneratorMap.put(
        RawSpanConstants.getValue(GRPC_HOST_PORT),
        (key, keyValue, builder, tagsMap) ->
            builder.getRequestBuilder().setHostPort(ValueConverter.getString(keyValue)));

    // grpc method
    RpcSemanticConventionUtils.getAttributeKeysForGrpcMethod()
        .forEach(
            v ->
                fieldGeneratorMap.put(
                    v,
                    (key, keyValue, builder, tagsMap) ->
                        builder.getRequestBuilder().setMethod(ValueConverter.getString(keyValue))));

    fieldGeneratorMap.put(
        RawSpanConstants.getValue(GRPC_ERROR_NAME),
        (key, keyValue, builder, tagsMap) ->
            builder.getResponseBuilder().setErrorName(ValueConverter.getString(keyValue)));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(GRPC_ERROR_MESSAGE),
        (key, keyValue, builder, tagsMap) ->
            builder.getResponseBuilder().setErrorMessage(ValueConverter.getString(keyValue)));

    fieldGeneratorMap.put(
        RawSpanConstants.getValue(GRPC_REQUEST_CALL_OPTIONS),
        (key, keyValue, builder, tagsMap) ->
            builder.getRequestBuilder().setCallOptions(ValueConverter.getString(keyValue)));

    // Response Status Code
    RpcSemanticConventionUtils.getAttributeKeysForGrpcStatusCode()
        .forEach(
            v ->
                fieldGeneratorMap.put(
                    v,
                    (key, keyValue, builder, tagsMap) -> setResponseStatusCode(builder, tagsMap)));

    // Response Status message
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE),
        (key, keyValue, builder, tagsMap) -> setResponseStatusMessage(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE),
        (key, keyValue, builder, tagsMap) -> setResponseStatusMessage(builder, tagsMap));

    // Metadata
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(GRPC_REQUEST_METADATA),
        (key, keyValue, builder, tagsMap) ->
            builder
                .getRequestBuilder()
                .getMetadata()
                .putAll(parseMetadataString(ValueConverter.getString(keyValue))));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(GRPC_RESPONSE_METADATA),
        (key, keyValue, builder, tagsMap) ->
            builder
                .getResponseBuilder()
                .getMetadata()
                .putAll(parseMetadataString(ValueConverter.getString(keyValue))));

    fieldGeneratorMap.put(
        RawSpanConstants.getValue(ENVOY_REQUEST_SIZE),
        (key, keyValue, builder, tagsMap) -> setRequestSize(builder, tagsMap));

    fieldGeneratorMap.put(
        RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE),
        (key, keyValue, builder, tagsMap) -> setResponseSize(builder, tagsMap));

    return fieldGeneratorMap;
  }

  private static Map<String, FieldGenerator<Grpc.Builder>> initializeRpcFieldGenerators() {
    Map<String, FieldGenerator<Grpc.Builder>> fieldGeneratorMap = new HashMap<>();

    fieldGeneratorMap.put(
        RPC_REQUEST_BODY.getValue(),
        (key, keyValue, builder, tagsMap) -> {
          builder.getRequestBuilder().setBody(ValueConverter.getString(keyValue));
          setRequestSize(builder, tagsMap);
        });
    fieldGeneratorMap.put(
        RPC_RESPONSE_BODY.getValue(),
        (key, keyValue, builder, tagsMap) -> {
          builder.getResponseBuilder().setBody(ValueConverter.getString(keyValue));
          setResponseSize(builder, tagsMap);
        });

    fieldGeneratorMap.put(
        RPC_ERROR_NAME.getValue(),
        (key, keyValue, builder, tagsMap) ->
            builder.getResponseBuilder().setErrorName(ValueConverter.getString(keyValue)));
    fieldGeneratorMap.put(
        RPC_ERROR_MESSAGE.getValue(),
        (key, keyValue, builder, tagsMap) ->
            builder.getResponseBuilder().setErrorMessage(ValueConverter.getString(keyValue)));

    // Response Status Code
    fieldGeneratorMap.put(
        RPC_STATUS_CODE.getValue(),
        (key, keyValue, builder, tagsMap) -> setResponseStatusCode(builder, tagsMap));

    fieldGeneratorMap.put(
        RPC_REQUEST_METADATA_X_FORWARDED_FOR.getValue(),
        (key, keyValue, builder, tagsMap) ->
            builder
                .getRequestBuilder()
                .getRequestMetadataBuilder()
                .setXForwardedFor(ValueConverter.getString(keyValue)));

    fieldGeneratorMap.put(
        RPC_REQUEST_METADATA_AUTHORITY.getValue(),
        (key, keyValue, builder, tagsMap) ->
            builder
                .getRequestBuilder()
                .getRequestMetadataBuilder()
                .setAuthority(ValueConverter.getString(keyValue)));

    fieldGeneratorMap.put(
        RPC_REQUEST_METADATA_CONTENT_TYPE.getValue(),
        (key, keyValue, builder, tagsMap) ->
            builder
                .getRequestBuilder()
                .getRequestMetadataBuilder()
                .setContentType(ValueConverter.getString(keyValue)));

    fieldGeneratorMap.put(
        RPC_REQUEST_METADATA_PATH.getValue(),
        (key, keyValue, builder, tagsMap) ->
            builder
                .getRequestBuilder()
                .getRequestMetadataBuilder()
                .setPath(ValueConverter.getString(keyValue)));
    fieldGeneratorMap.put(
        RPC_REQUEST_METADATA_USER_AGENT.getValue(),
        (key, keyValue, builder, tagsMap) ->
            builder
                .getRequestBuilder()
                .getRequestMetadataBuilder()
                .setUserAgent(ValueConverter.getString(keyValue)));

    fieldGeneratorMap.put(
        RPC_RESPONSE_METADATA_CONTENT_TYPE.getValue(),
        (key, keyValue, builder, tagsMap) ->
            builder
                .getResponseBuilder()
                .getResponseMetadataBuilder()
                .setContentType(ValueConverter.getString(keyValue)));

    fieldGeneratorMap.put(
        RPC_REQUEST_METADATA_CONTENT_LENGTH.getValue(),
        (key, keyValue, builder, tagsMap) -> setRequestSize(builder, tagsMap));

    fieldGeneratorMap.put(
        RPC_RESPONSE_METADATA_CONTENT_LENGTH.getValue(),
        (key, keyValue, builder, tagsMap) -> setResponseSize(builder, tagsMap));

    return fieldGeneratorMap;
  }

  private static void setResponseStatusCode(
      Grpc.Builder grpcBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (grpcBuilder.getResponseBuilder().hasStatusCode()) {
      return;
    }

    FirstMatchingKeyFinder.getIntegerValueByFirstMatchingKey(
            tagsMap, RpcSemanticConventionUtils.getAttributeKeysForGrpcStatusCode())
        .ifPresent(statusCode -> grpcBuilder.getResponseBuilder().setStatusCode(statusCode));
  }

  private static void setResponseStatusMessage(
      Grpc.Builder grpcBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (grpcBuilder.getResponseBuilder().hasStatusMessage()) {
      return;
    }

    FirstMatchingKeyFinder.getStringValueByFirstMatchingKey(tagsMap, STATUS_MESSAGE_ATTRIBUTES)
        .ifPresent(
            statusMessage -> grpcBuilder.getResponseBuilder().setStatusMessage(statusMessage));
  }

  private static void setRequestSize(
      Grpc.Builder grpcBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (tagsMap.containsKey(RawSpanConstants.getValue(ENVOY_REQUEST_SIZE))) {
      grpcBuilder
          .getRequestBuilder()
          .setSize(
              ValueConverter.getInteger(
                  tagsMap.get(RawSpanConstants.getValue(ENVOY_REQUEST_SIZE))));
    } else if (tagsMap.containsKey(RPC_REQUEST_METADATA_CONTENT_LENGTH.getValue())) {
      grpcBuilder
          .getRequestBuilder()
          .setSize(
              ValueConverter.getInteger(
                  tagsMap.get(RPC_REQUEST_METADATA_CONTENT_LENGTH.getValue())));
    } else if (tagsMap.containsKey(RawSpanConstants.getValue(GRPC_REQUEST_BODY))
        && !isGrpcRequestBodyTruncated(tagsMap)) {
      String requestBody =
          ValueConverter.getString(tagsMap.get(RawSpanConstants.getValue(GRPC_REQUEST_BODY)));
      grpcBuilder.getRequestBuilder().setSize(requestBody.length());
    } else if (tagsMap.containsKey(RPC_REQUEST_BODY.getValue())
        && !isRpcRequestBodyTruncated(tagsMap)) {
      String requestBody = ValueConverter.getString(tagsMap.get(RPC_REQUEST_BODY.getValue()));
      grpcBuilder.getRequestBuilder().setSize(requestBody.length());
    }
  }

  private static boolean isGrpcRequestBodyTruncated(
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (tagsMap.containsKey(RawSpanConstants.getValue(GRPC_REQUEST_BODY_TRUNCATED))) {
      return ValueConverter.getBoolean(
          tagsMap.get(RawSpanConstants.getValue(GRPC_REQUEST_BODY_TRUNCATED)));
    }
    return false;
  }

  private static boolean isRpcRequestBodyTruncated(
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (tagsMap.containsKey(RPC_REQUEST_BODY_TRUNCATED.getValue())) {
      return ValueConverter.getBoolean(tagsMap.get(RPC_REQUEST_BODY_TRUNCATED.getValue()));
    }
    return false;
  }

  private static void setResponseSize(
      Grpc.Builder grpcBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (tagsMap.containsKey(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE))) {
      grpcBuilder
          .getResponseBuilder()
          .setSize(
              ValueConverter.getInteger(
                  tagsMap.get(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE))));
    } else if (tagsMap.containsKey(RPC_RESPONSE_METADATA_CONTENT_LENGTH.getValue())) {
      grpcBuilder
          .getResponseBuilder()
          .setSize(
              ValueConverter.getInteger(
                  tagsMap.get(RPC_RESPONSE_METADATA_CONTENT_LENGTH.getValue())));
    } else if (tagsMap.containsKey(RawSpanConstants.getValue(GRPC_RESPONSE_BODY))
        && !isGrpcResponseBodyTruncated(tagsMap)) {
      String responseBody =
          ValueConverter.getString(tagsMap.get(RawSpanConstants.getValue(GRPC_RESPONSE_BODY)));
      grpcBuilder.getResponseBuilder().setSize(responseBody.length());
    } else if (tagsMap.containsKey(RPC_RESPONSE_BODY.getValue())
        && !isRpcResponseBodyTruncated(tagsMap)) {
      String responseBody = ValueConverter.getString(tagsMap.get(RPC_RESPONSE_BODY.getValue()));
      grpcBuilder.getResponseBuilder().setSize(responseBody.length());
    }
  }

  private static boolean isGrpcResponseBodyTruncated(
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (tagsMap.containsKey(RawSpanConstants.getValue(GRPC_RESPONSE_BODY_TRUNCATED))) {
      return ValueConverter.getBoolean(
          tagsMap.get(RawSpanConstants.getValue(GRPC_RESPONSE_BODY_TRUNCATED)));
    }
    return false;
  }

  private static boolean isRpcResponseBodyTruncated(
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (tagsMap.containsKey(RPC_RESPONSE_BODY_TRUNCATED.getValue())) {
      return ValueConverter.getBoolean(tagsMap.get(RPC_RESPONSE_BODY_TRUNCATED.getValue()));
    }
    return false;
  }

  private static Map<String, String> parseMetadataString(String metadataString) {
    if (!metadataString.startsWith(METADATA_STR_VAL_PREFIX)) {
      return EMPTY_METADATA_MAP;
    }

    int metadataStartIndex = metadataString.indexOf('(') + 1;
    int metadataEndIndex =
        metadataString.length()
            - 1; // The last character is ')'. So the end index is the 2nd last character.
    String actualMetadataString = metadataString.substring(metadataStartIndex, metadataEndIndex);
    String[] metadataArr = actualMetadataString.split(METADATA_DELIMITER);
    Map<String, String> metadataMap = new HashMap<>();
    for (String metadata : metadataArr) {
      String[] metadataKeyValue = metadata.split(METADATA_KEY_VALUE_DELIMITER);
      if (metadataKeyValue.length != 2) {
        continue;
      }
      metadataMap.put(metadataKeyValue[0], metadataKeyValue[1]);
    }

    return metadataMap;
  }

  @Override
  protected Grpc.Builder getProtocolBuilder(Event.Builder eventBuilder) {
    Grpc.Builder grpcBuilder = eventBuilder.getGrpcBuilder();

    if (!grpcBuilder.getRequestBuilder().hasMetadata()
        || !grpcBuilder.getResponseBuilder().hasMetadata()
        || !grpcBuilder.getRequestBuilder().getRequestMetadataBuilder().hasOtherMetadata()
        || !grpcBuilder.getResponseBuilder().getResponseMetadataBuilder().hasOtherMetadata()) {
      grpcBuilder.getRequestBuilder().setMetadata(new HashMap<>());
      grpcBuilder.getResponseBuilder().setMetadata(new HashMap<>());
      grpcBuilder.getRequestBuilder().getRequestMetadataBuilder().setOtherMetadata(new HashMap<>());
      grpcBuilder
          .getResponseBuilder()
          .getResponseMetadataBuilder()
          .setOtherMetadata(new HashMap<>());
    }

    return grpcBuilder;
  }

  @Override
  protected Map<String, FieldGenerator<Grpc.Builder>> getFieldGeneratorMap() {
    return fieldGeneratorMap;
  }

  protected Map<String, FieldGenerator<Grpc.Builder>> getRpcFieldGeneratorMap() {
    return rpcFieldGeneratorMap;
  }

  protected void handleRpcRequestMetadata(
      String key, JaegerSpanInternalModel.KeyValue keyValue, Grpc.Builder grpcBuilder) {
    grpcBuilder
        .getRequestBuilder()
        .getRequestMetadataBuilder()
        .getOtherMetadata()
        .put(key, ValueConverter.getString(keyValue));
    grpcBuilder.getRequestBuilder().getMetadata().put(key, ValueConverter.getString(keyValue));
  }

  protected void handleRpcResponseMetadata(
      String key, JaegerSpanInternalModel.KeyValue keyValue, Grpc.Builder grpcBuilder) {
    grpcBuilder
        .getResponseBuilder()
        .getResponseMetadataBuilder()
        .getOtherMetadata()
        .put(key, ValueConverter.getString(keyValue));
    grpcBuilder.getResponseBuilder().getMetadata().put(key, ValueConverter.getString(keyValue));
  }

  protected void populateOtherFields(
      Event.Builder eventBuilder, final Map<String, AttributeValue> attributeValueMap) {
    maybeSetGrpcHostPortForOtelFormat(eventBuilder, attributeValueMap);
    maybeSetGrpcExceptionForOtelFormat(eventBuilder, attributeValueMap);
  }

  protected void maybeSetGrpcExceptionForOtelFormat(
      Event.Builder builder, final Map<String, AttributeValue> attributeValueMap) {
    if (RpcSemanticConventionUtils.isRpcTypeGrpcForOTelFormat(attributeValueMap)) {
      if (attributeValueMap.containsKey(OTelErrorSemanticConventions.EXCEPTION_TYPE.getValue())) {
        builder
            .getGrpcBuilder()
            .getResponseBuilder()
            .setErrorName(
                attributeValueMap
                    .get(OTelErrorSemanticConventions.EXCEPTION_TYPE.getValue())
                    .getValue());
      }
      if (attributeValueMap.containsKey(
          OTelErrorSemanticConventions.EXCEPTION_MESSAGE.getValue())) {
        builder
            .getGrpcBuilder()
            .getResponseBuilder()
            .setErrorMessage(
                attributeValueMap
                    .get(OTelErrorSemanticConventions.EXCEPTION_MESSAGE.getValue())
                    .getValue());
      }
    }
  }

  protected void maybeSetGrpcHostPortForOtelFormat(
      Event.Builder builder, final Map<String, AttributeValue> attributeValueMap) {
    Optional<String> grpcHostPort = RpcSemanticConventionUtils.getGrpcURI(attributeValueMap);
    grpcHostPort.ifPresent(s -> builder.getGrpcBuilder().getRequestBuilder().setHostPort(s));
  }
}
