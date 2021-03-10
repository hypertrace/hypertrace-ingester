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
