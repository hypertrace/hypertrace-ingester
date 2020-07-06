package org.hypertrace.core.spannormalizer.fieldgenerators;

import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_GRPC_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_ERROR_NAME;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_HOST_PORT;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_METHOD;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_CALL_OPTIONS;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_METADATA;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_METADATA;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_STATUS_CODE;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.grpc.Grpc;
import org.hypertrace.core.span.constants.RawSpanConstants;

public class GrpcFieldsGenerator extends ProtocolFieldsGenerator<Grpc.Builder> {
  private static final String METADATA_STR_VAL_PREFIX = "Metadata(";
  private static Map<String, FieldGenerator<Grpc.Builder>> fieldGeneratorMap =
      initializeFieldGenerators();

  private static final List<String> STATUS_CODE_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_CODE),
          RawSpanConstants.getValue(GRPC_STATUS_CODE),
          RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE));

  private static final List<String> STATUS_MESSAGE_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE),
          RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE));

  private static final Map<String, String> EMPTY_METADATA_MAP = Map.of();
  private static final String METADATA_DELIMITER = ",";
  private static final String METADATA_KEY_VALUE_DELIMITER = "=";

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
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(GRPC_METHOD),
        (key, keyValue, builder, tagsMap) ->
            builder.getRequestBuilder().setMethod(ValueConverter.getString(keyValue)));
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
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_CODE),
        (key, keyValue, builder, tagsMap) -> setResponseStatusCode(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(GRPC_STATUS_CODE),
        (key, keyValue, builder, tagsMap) -> setResponseStatusCode(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE),
        (key, keyValue, builder, tagsMap) -> setResponseStatusCode(builder, tagsMap));

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

    return fieldGeneratorMap;
  }

  @Override
  protected Grpc.Builder getProtocolBuilder(Event.Builder eventBuilder) {
    Grpc.Builder grpcBuilder = eventBuilder.getGrpcBuilder();

    if (!grpcBuilder.getRequestBuilder().hasMetadata()
        || !grpcBuilder.getResponseBuilder().hasMetadata()) {
      grpcBuilder.getRequestBuilder().setMetadata(new HashMap<>());
      grpcBuilder.getResponseBuilder().setMetadata(new HashMap<>());
    }

    return grpcBuilder;
  }

  @Override
  protected Map<String, FieldGenerator<Grpc.Builder>> getFieldGeneratorMap() {
    return fieldGeneratorMap;
  }

  private static void setResponseStatusCode(
      Grpc.Builder grpcBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (grpcBuilder.getResponseBuilder().hasStatusCode()) {
      return;
    }

    FirstMatchingKeyFinder.getIntegerValueByFirstMatchingKey(tagsMap, STATUS_CODE_ATTRIBUTES)
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
    } else if (tagsMap.containsKey(RawSpanConstants.getValue(GRPC_REQUEST_BODY))) {
      String requestBody =
          ValueConverter.getString(tagsMap.get(RawSpanConstants.getValue(GRPC_REQUEST_BODY)));
      grpcBuilder.getRequestBuilder().setSize(requestBody.length());
    }
  }

  private static void setResponseSize(
      Grpc.Builder grpcBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (tagsMap.containsKey(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE))) {
      grpcBuilder
          .getResponseBuilder()
          .setSize(
              ValueConverter.getInteger(
                  tagsMap.get(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE))));
    } else if (tagsMap.containsKey(RawSpanConstants.getValue(GRPC_RESPONSE_BODY))) {
      String responseBody =
          ValueConverter.getString(tagsMap.get(RawSpanConstants.getValue(GRPC_RESPONSE_BODY)));
      grpcBuilder.getResponseBuilder().setSize(responseBody.length());
    }
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
}
