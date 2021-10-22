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
import static org.hypertrace.core.span.normalizer.constants.OTelRpcSystem.OTEL_RPC_SYSTEM_GRPC;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SYSTEM;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_BODY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_CONTENT_LENGTH;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_BODY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_METADATA_CONTENT_LENGTH;
import static org.hypertrace.core.spannormalizer.utils.TestUtils.createKeyValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Maps;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.grpc.Grpc;
import org.hypertrace.core.semantic.convention.constants.error.OTelErrorSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.rpc.OTelRpcSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GrpcFieldsGeneratorTest {
  @Test
  public void testGrpcFieldsConverter() {
    String requestBody = "some grpc request body";
    String responseBody = "some grpc response body";
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(RawSpanConstants.getValue(GRPC_REQUEST_BODY), createKeyValue(requestBody));
    tagsMap.put(RawSpanConstants.getValue(GRPC_RESPONSE_BODY), createKeyValue(responseBody));
    tagsMap.put(RawSpanConstants.getValue(GRPC_HOST_PORT), createKeyValue("localhost:50051"));
    tagsMap.put(RawSpanConstants.getValue(GRPC_METHOD), createKeyValue("GET"));
    tagsMap.put(RawSpanConstants.getValue(GRPC_ERROR_NAME), createKeyValue("Some error name"));
    tagsMap.put(
        RawSpanConstants.getValue(GRPC_ERROR_MESSAGE), createKeyValue("Some error message"));
    tagsMap.put(
        RawSpanConstants.getValue(GRPC_REQUEST_CALL_OPTIONS),
        createKeyValue("Request call options"));
    tagsMap.put(RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_CODE), createKeyValue(12));
    tagsMap.put(RawSpanConstants.getValue(GRPC_STATUS_CODE), createKeyValue(13));
    tagsMap.put(RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE), createKeyValue(14));
    tagsMap.put(
        RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE),
        createKeyValue("CENSUS_RESPONSE_STATUS_MESSAGE"));
    tagsMap.put(
        RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE),
        createKeyValue("ENVOY_GRPC_STATUS_MESSAGE"));
    tagsMap.put(
        RawSpanConstants.getValue(GRPC_REQUEST_METADATA),
        createKeyValue("Metadata(authorization=Bearer some-jwt,x-some-customer-header=hval)"));
    tagsMap.put(
        RawSpanConstants.getValue(GRPC_RESPONSE_METADATA),
        createKeyValue("Metadata(x-response-header-1=header val,x-some-other=hval_response)"));

    GrpcFieldsGenerator grpcFieldsGenerator = new GrpcFieldsGenerator();
    Event.Builder eventBuilder = Event.newBuilder();
    Grpc.Builder grpcBuilder = grpcFieldsGenerator.getProtocolBuilder(eventBuilder);

    Assertions.assertSame(eventBuilder.getGrpcBuilder(), grpcBuilder);

    tagsMap.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap));

    Assertions.assertEquals("some grpc request body", grpcBuilder.getRequestBuilder().getBody());
    Assertions.assertEquals("some grpc response body", grpcBuilder.getResponseBuilder().getBody());
    Assertions.assertEquals("localhost:50051", grpcBuilder.getRequestBuilder().getHostPort());
    Assertions.assertEquals("GET", grpcBuilder.getRequestBuilder().getMethod());
    Assertions.assertEquals("Some error name", grpcBuilder.getResponseBuilder().getErrorName());
    Assertions.assertEquals(
        "Some error message", grpcBuilder.getResponseBuilder().getErrorMessage());
    Assertions.assertEquals(
        "Request call options", grpcBuilder.getRequestBuilder().getCallOptions());
    Assertions.assertEquals(12, grpcBuilder.getResponseBuilder().getStatusCode());
    Assertions.assertEquals(
        "CENSUS_RESPONSE_STATUS_MESSAGE", grpcBuilder.getResponseBuilder().getStatusMessage());
    Assertions.assertEquals(requestBody.length(), grpcBuilder.getRequestBuilder().getSize());
    Assertions.assertEquals(responseBody.length(), grpcBuilder.getResponseBuilder().getSize());
    Assertions.assertEquals(
        Map.of(
            "authorization", "Bearer some-jwt",
            "x-some-customer-header", "hval"),
        grpcBuilder.getRequestBuilder().getMetadata());
    Assertions.assertEquals(
        Map.of(
            "x-response-header-1", "header val",
            "x-some-other", "hval_response"),
        grpcBuilder.getResponseBuilder().getMetadata());
  }

  @Test
  public void testGrpcFieldsConverterEnvoyRequestAndResponseSizeHigherPriority() {
    String requestBody = "some grpc request body";
    String responseBody = "some grpc response body";
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(RawSpanConstants.getValue(GRPC_REQUEST_BODY), createKeyValue(requestBody));
    tagsMap.put(RawSpanConstants.getValue(GRPC_RESPONSE_BODY), createKeyValue(responseBody));
    tagsMap.put(RawSpanConstants.getValue(ENVOY_REQUEST_SIZE), createKeyValue(200));
    tagsMap.put(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE), createKeyValue(400));

    GrpcFieldsGenerator grpcFieldsGenerator = new GrpcFieldsGenerator();
    Event.Builder eventBuilder = Event.newBuilder();
    Grpc.Builder grpcBuilder = grpcFieldsGenerator.getProtocolBuilder(eventBuilder);

    Assertions.assertSame(eventBuilder.getGrpcBuilder(), grpcBuilder);

    grpcFieldsGenerator.addValueToBuilder(
        RawSpanConstants.getValue(GRPC_REQUEST_BODY),
        tagsMap.get(RawSpanConstants.getValue(GRPC_REQUEST_BODY)),
        eventBuilder,
        tagsMap);
    grpcFieldsGenerator.addValueToBuilder(
        RawSpanConstants.getValue(GRPC_RESPONSE_BODY),
        tagsMap.get(RawSpanConstants.getValue(GRPC_RESPONSE_BODY)),
        eventBuilder,
        tagsMap);

    Assertions.assertEquals("some grpc request body", grpcBuilder.getRequestBuilder().getBody());
    Assertions.assertEquals("some grpc response body", grpcBuilder.getResponseBuilder().getBody());
    Assertions.assertEquals(200, grpcBuilder.getRequestBuilder().getSize());
    Assertions.assertEquals(400, grpcBuilder.getResponseBuilder().getSize());
  }

  @Test
  public void testGrpcRequestSizeTagKeysPriority() {
    GrpcFieldsGenerator grpcFieldsGenerator = new GrpcFieldsGenerator();
    RpcFieldsGenerator rpcFieldsGenerator = new RpcFieldsGenerator(grpcFieldsGenerator);

    // test 1: respect ENVOY_REQUEST_SIZE
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap1 = new HashMap<>();
    tagsMap1.put(RawSpanConstants.getValue(ENVOY_REQUEST_SIZE), createKeyValue(100));
    tagsMap1.put(
        RawSpanConstants.getValue(GRPC_REQUEST_BODY), createKeyValue("Hey Grpc, you there!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> rpcTagsMap1 = new HashMap<>();
    rpcTagsMap1.put(
        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), createKeyValue(OTEL_RPC_SYSTEM_GRPC.getValue()));
    rpcTagsMap1.put(RPC_REQUEST_METADATA_CONTENT_LENGTH.getValue(), createKeyValue(200));
    rpcTagsMap1.put(RPC_REQUEST_BODY.getValue(), createKeyValue("Hey rpc, you there!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> combinedTags1 = new HashMap<>();
    combinedTags1.putAll(tagsMap1);
    combinedTags1.putAll(rpcTagsMap1);

    Event.Builder eventBuilder1 = Event.newBuilder();
    Grpc.Builder grpcBuilder1 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder1);

    tagsMap1.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder1, combinedTags1));

    rpcTagsMap1.forEach(
        (key, keyValue) ->
            rpcFieldsGenerator.handleKeyIfNecessary(key, keyValue, eventBuilder1, combinedTags1));

    assertEquals(100, grpcBuilder1.getRequestBuilder().getSize());

    // test 2: respect RPC_REQUEST_METADATA_CONTENT_LENGTH
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(
        RawSpanConstants.getValue(GRPC_REQUEST_BODY), createKeyValue("Hey Grpc, you there!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> rpcTagsMap2 = new HashMap<>();
    rpcTagsMap2.put(
        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), createKeyValue(OTEL_RPC_SYSTEM_GRPC.getValue()));
    rpcTagsMap2.put(RPC_REQUEST_METADATA_CONTENT_LENGTH.getValue(), createKeyValue(200));
    rpcTagsMap2.put(RPC_REQUEST_BODY.getValue(), createKeyValue("Hey rpc, you there!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> combinedTags2 = new HashMap<>();
    combinedTags2.putAll(tagsMap2);
    combinedTags2.putAll(rpcTagsMap2);

    Event.Builder eventBuilder2 = Event.newBuilder();
    Grpc.Builder grpcBuilder2 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder2);

    tagsMap2.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, combinedTags2));

    rpcTagsMap2.forEach(
        (key, keyValue) ->
            rpcFieldsGenerator.handleKeyIfNecessary(key, keyValue, eventBuilder2, combinedTags2));

    assertEquals(200, grpcBuilder2.getRequestBuilder().getSize());

    // test 3: respect GRPC_REQUEST_BODY
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap3 = new HashMap<>();
    tagsMap3.put(
        RawSpanConstants.getValue(GRPC_REQUEST_BODY), createKeyValue("Hey Grpc, you there!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> rpcTagsMap3 = new HashMap<>();
    rpcTagsMap3.put(
        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), createKeyValue(OTEL_RPC_SYSTEM_GRPC.getValue()));
    rpcTagsMap3.put(RPC_REQUEST_BODY.getValue(), createKeyValue("Hey rpc, you there!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> combinedTags3 = new HashMap<>();
    combinedTags3.putAll(tagsMap3);
    combinedTags3.putAll(rpcTagsMap3);

    Event.Builder eventBuilder3 = Event.newBuilder();
    Grpc.Builder grpcBuilder3 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder3);

    tagsMap3.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder3, combinedTags3));

    rpcTagsMap3.forEach(
        (key, keyValue) ->
            rpcFieldsGenerator.handleKeyIfNecessary(key, keyValue, eventBuilder3, combinedTags3));

    assertEquals(21, grpcBuilder3.getRequestBuilder().getSize());

    // test 4: respect RPC_REQUEST_BODY
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap4 = new HashMap<>();
    Map<String, JaegerSpanInternalModel.KeyValue> rpcTagsMap4 = new HashMap<>();
    rpcTagsMap4.put(
        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), createKeyValue(OTEL_RPC_SYSTEM_GRPC.getValue()));
    rpcTagsMap4.put(RPC_REQUEST_BODY.getValue(), createKeyValue("Hey rpc, you there!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> combinedTags4 = new HashMap<>();
    combinedTags4.putAll(tagsMap4);
    combinedTags4.putAll(rpcTagsMap4);

    Event.Builder eventBuilder4 = Event.newBuilder();
    Grpc.Builder grpcBuilder4 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder4);

    tagsMap3.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder4, combinedTags4));

    rpcTagsMap3.forEach(
        (key, keyValue) ->
            rpcFieldsGenerator.handleKeyIfNecessary(key, keyValue, eventBuilder4, combinedTags4));

    assertEquals(20, grpcBuilder4.getRequestBuilder().getSize());
  }

  @Test
  public void testGrpcResponseSizeTagKeysPriority() {
    GrpcFieldsGenerator grpcFieldsGenerator = new GrpcFieldsGenerator();
    RpcFieldsGenerator rpcFieldsGenerator = new RpcFieldsGenerator(grpcFieldsGenerator);

    // test 1: respect ENVOY_RESPONSE_SIZE
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap1 = new HashMap<>();
    tagsMap1.put(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE), createKeyValue(10));
    tagsMap1.put(
        RawSpanConstants.getValue(GRPC_RESPONSE_BODY), createKeyValue("Hello from grpc!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> rpcTagsMap1 = new HashMap<>();
    rpcTagsMap1.put(
        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), createKeyValue(OTEL_RPC_SYSTEM_GRPC.getValue()));
    rpcTagsMap1.put(RPC_RESPONSE_METADATA_CONTENT_LENGTH.getValue(), createKeyValue(20));
    rpcTagsMap1.put(RPC_RESPONSE_BODY.getValue(), createKeyValue("Hello from rpc!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> combinedTags1 = new HashMap<>();
    combinedTags1.putAll(tagsMap1);
    combinedTags1.putAll(rpcTagsMap1);

    Event.Builder eventBuilder1 = Event.newBuilder();
    Grpc.Builder grpcBuilder1 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder1);

    tagsMap1.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder1, combinedTags1));

    rpcTagsMap1.forEach(
        (key, keyValue) ->
            rpcFieldsGenerator.handleKeyIfNecessary(key, keyValue, eventBuilder1, combinedTags1));

    assertEquals(10, grpcBuilder1.getResponseBuilder().getSize());

    // test 2: respect RPC_RESPONSE_METADATA_CONTENT_LENGTH
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(
        RawSpanConstants.getValue(GRPC_RESPONSE_BODY), createKeyValue("Hello from grpc!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> rpcTagsMap2 = new HashMap<>();
    rpcTagsMap2.put(
        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), createKeyValue(OTEL_RPC_SYSTEM_GRPC.getValue()));
    rpcTagsMap2.put(RPC_RESPONSE_METADATA_CONTENT_LENGTH.getValue(), createKeyValue(200));
    rpcTagsMap2.put(RPC_RESPONSE_BODY.getValue(), createKeyValue("Hello from rpc!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> combinedTags2 = new HashMap<>();
    combinedTags2.putAll(tagsMap2);
    combinedTags2.putAll(rpcTagsMap2);

    Event.Builder eventBuilder2 = Event.newBuilder();
    Grpc.Builder grpcBuilder2 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder2);

    tagsMap2.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, combinedTags2));

    rpcTagsMap2.forEach(
        (key, keyValue) ->
            rpcFieldsGenerator.handleKeyIfNecessary(key, keyValue, eventBuilder2, combinedTags2));

    assertEquals(200, grpcBuilder2.getResponseBuilder().getSize());

    // test 3: respect GRPC_RESPONSE_BODY
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap3 = new HashMap<>();
    tagsMap3.put(
        RawSpanConstants.getValue(GRPC_RESPONSE_BODY), createKeyValue("Hello from grpc!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> rpcTagsMap3 = new HashMap<>();
    rpcTagsMap3.put(
        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), createKeyValue(OTEL_RPC_SYSTEM_GRPC.getValue()));
    rpcTagsMap3.put(RPC_RESPONSE_BODY.getValue(), createKeyValue("Hello from rpc!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> combinedTags3 = new HashMap<>();
    combinedTags3.putAll(tagsMap3);
    combinedTags3.putAll(rpcTagsMap3);

    Event.Builder eventBuilder3 = Event.newBuilder();
    Grpc.Builder grpcBuilder3 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder3);

    tagsMap3.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder3, combinedTags3));

    rpcTagsMap3.forEach(
        (key, keyValue) ->
            rpcFieldsGenerator.handleKeyIfNecessary(key, keyValue, eventBuilder3, combinedTags3));

    assertEquals(17, grpcBuilder3.getResponseBuilder().getSize());

    // test 4: respect RPC_RESPONSE_BODY
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap4 = new HashMap<>();
    Map<String, JaegerSpanInternalModel.KeyValue> rpcTagsMap4 = new HashMap<>();
    rpcTagsMap4.put(
        OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), createKeyValue(OTEL_RPC_SYSTEM_GRPC.getValue()));
    rpcTagsMap4.put(RPC_RESPONSE_BODY.getValue(), createKeyValue("Hello from rpc!!"));

    Map<String, JaegerSpanInternalModel.KeyValue> combinedTags4 = new HashMap<>();
    combinedTags4.putAll(tagsMap4);
    combinedTags4.putAll(rpcTagsMap4);

    Event.Builder eventBuilder4 = Event.newBuilder();
    Grpc.Builder grpcBuilder4 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder4);

    tagsMap3.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder4, combinedTags4));

    rpcTagsMap3.forEach(
        (key, keyValue) ->
            rpcFieldsGenerator.handleKeyIfNecessary(key, keyValue, eventBuilder4, combinedTags4));

    assertEquals(16, grpcBuilder4.getResponseBuilder().getSize());
  }

  @Test
  public void testGrpcFieldsConverterStatusCodePriority() {
    GrpcFieldsGenerator grpcFieldsGenerator = new GrpcFieldsGenerator();

    // CENSUS_RESPONSE_STATUS_CODE is highest priority
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_CODE), createKeyValue(12));
    tagsMap.put(RawSpanConstants.getValue(GRPC_STATUS_CODE), createKeyValue(13));
    tagsMap.put(RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE), createKeyValue(14));

    Event.Builder eventBuilder = Event.newBuilder();
    Grpc.Builder grpcBuilder = grpcFieldsGenerator.getProtocolBuilder(eventBuilder);

    tagsMap.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap));

    Assertions.assertSame(eventBuilder.getGrpcBuilder(), grpcBuilder);
    Assertions.assertEquals(12, grpcBuilder.getResponseBuilder().getStatusCode());

    // Then GRPC_STATUS_CODE
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(RawSpanConstants.getValue(GRPC_STATUS_CODE), createKeyValue(13));
    tagsMap2.put(RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE), createKeyValue(14));

    Event.Builder eventBuilder2 = Event.newBuilder();
    Grpc.Builder grpcBuilder2 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder2);
    Assertions.assertSame(eventBuilder2.getGrpcBuilder(), grpcBuilder2);

    tagsMap.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, tagsMap2));

    Assertions.assertEquals(13, grpcBuilder2.getResponseBuilder().getStatusCode());

    // Then CENSUS_RESPONSE_CENSUS_STATUS_CODE
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap3 = new HashMap<>();
    tagsMap3.put(RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE), createKeyValue(14));

    Event.Builder eventBuilder3 = Event.newBuilder();
    Grpc.Builder grpcBuilder3 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder3);
    Assertions.assertSame(eventBuilder3.getGrpcBuilder(), grpcBuilder3);

    tagsMap.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder3, tagsMap3));

    Assertions.assertEquals(14, grpcBuilder3.getResponseBuilder().getStatusCode());
  }

  @Test
  public void testGrpcFieldsConverterStatusMessagePriority() {
    GrpcFieldsGenerator grpcFieldsGenerator = new GrpcFieldsGenerator();

    // CENSUS_RESPONSE_STATUS_MESSAGE is highest priority
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(
        RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE),
        createKeyValue("CENSUS_RESPONSE_STATUS_MESSAGE"));
    tagsMap.put(
        RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE),
        createKeyValue("ENVOY_GRPC_STATUS_MESSAGE"));

    Event.Builder eventBuilder = Event.newBuilder();
    Grpc.Builder grpcBuilder = grpcFieldsGenerator.getProtocolBuilder(eventBuilder);

    tagsMap.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap));

    Assertions.assertSame(eventBuilder.getGrpcBuilder(), grpcBuilder);
    Assertions.assertEquals(
        "CENSUS_RESPONSE_STATUS_MESSAGE", grpcBuilder.getResponseBuilder().getStatusMessage());

    // Then ENVOY_GRPC_STATUS_MESSAGE
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(
        RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE),
        createKeyValue("ENVOY_GRPC_STATUS_MESSAGE"));

    Event.Builder eventBuilder2 = Event.newBuilder();
    Grpc.Builder grpcBuilder2 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder2);
    Assertions.assertSame(eventBuilder2.getGrpcBuilder(), grpcBuilder2);

    tagsMap.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, tagsMap2));

    Assertions.assertEquals(
        "ENVOY_GRPC_STATUS_MESSAGE", grpcBuilder2.getResponseBuilder().getStatusMessage());
  }

  @Test
  public void testGrpcFieldsConverterInvalidMetadataString() {
    GrpcFieldsGenerator grpcFieldsGenerator = new GrpcFieldsGenerator();

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(
        RawSpanConstants.getValue(GRPC_REQUEST_METADATA),
        createKeyValue("authorization=Bearer some-jwt,x-some-customer-header=hval"));

    Event.Builder eventBuilder = Event.newBuilder();
    Grpc.Builder grpcBuilder = grpcFieldsGenerator.getProtocolBuilder(eventBuilder);

    tagsMap.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap));

    Assertions.assertEquals(Map.of(), grpcBuilder.getRequestBuilder().getMetadata());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(
        RawSpanConstants.getValue(GRPC_REQUEST_METADATA),
        createKeyValue("Metadata(authorization Bearer some-jwt"));

    Event.Builder eventBuilder2 = Event.newBuilder();
    Grpc.Builder grpcBuilder2 = grpcFieldsGenerator.getProtocolBuilder(eventBuilder2);

    tagsMap2.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, tagsMap2));

    Assertions.assertEquals(Map.of(), grpcBuilder2.getRequestBuilder().getMetadata());
  }

  @Test
  public void testGrpcFieldsConverterForOTelSpan() {
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(OTelRpcSemanticConventions.RPC_METHOD.getValue(), createKeyValue("GET"));
    tagsMap.put(OTelRpcSemanticConventions.GRPC_STATUS_CODE.getValue(), createKeyValue(5));

    GrpcFieldsGenerator grpcFieldsGenerator = new GrpcFieldsGenerator();
    Event.Builder eventBuilder = Event.newBuilder();
    Grpc.Builder grpcBuilder = grpcFieldsGenerator.getProtocolBuilder(eventBuilder);

    Assertions.assertSame(eventBuilder.getGrpcBuilder(), grpcBuilder);

    tagsMap.forEach(
        (key, keyValue) ->
            grpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap));

    Assertions.assertEquals("GET", grpcBuilder.getRequestBuilder().getMethod());
    Assertions.assertEquals(5, grpcBuilder.getResponseBuilder().getStatusCode());
  }

  @Test
  public void testPopulateOtherFields() {
    GrpcFieldsGenerator grpcFieldsGenerator = new GrpcFieldsGenerator();
    Event.Builder eventBuilder = Event.newBuilder();
    Map<String, AttributeValue> map = Maps.newHashMap();
    map.put(
        OTelRpcSemanticConventions.RPC_SYSTEM.getValue(),
        AttributeValue.newBuilder()
            .setValue(OTelRpcSemanticConventions.RPC_SYSTEM_VALUE_GRPC.getValue())
            .build());
    map.put(
        OTelSpanSemanticConventions.NET_PEER_IP.getValue(),
        AttributeValue.newBuilder().setValue(("172.0.1.17")).build());
    map.put(
        OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
        AttributeValue.newBuilder().setValue("example.com").build());
    map.put(
        OTelSpanSemanticConventions.NET_PEER_PORT.getValue(),
        AttributeValue.newBuilder().setValue("2705").build());
    map.put(
        OTelErrorSemanticConventions.EXCEPTION_MESSAGE.getValue(),
        AttributeValue.newBuilder().setValue("resource not found").build());
    map.put(
        OTelErrorSemanticConventions.EXCEPTION_TYPE.getValue(),
        AttributeValue.newBuilder().setValue("NPE").build());
    grpcFieldsGenerator.populateOtherFields(eventBuilder, map);
    assertEquals(
        "example.com:2705", eventBuilder.getGrpcBuilder().getRequestBuilder().getHostPort());
    assertEquals(
        "resource not found", eventBuilder.getGrpcBuilder().getResponseBuilder().getErrorMessage());
    assertEquals("NPE", eventBuilder.getGrpcBuilder().getResponseBuilder().getErrorName());
  }
}
