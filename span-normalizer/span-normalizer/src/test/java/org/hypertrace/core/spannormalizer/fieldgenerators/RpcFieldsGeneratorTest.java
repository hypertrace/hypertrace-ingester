package org.hypertrace.core.spannormalizer.fieldgenerators;

import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_METHOD;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SERVICE;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SYSTEM;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_ERROR_NAME;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_BODY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_AUTHORITY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_CONTENT_TYPE;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_PATH;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_USER_AGENT;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_X_FORWARDED_FOR;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_BODY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_METADATA;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_METADATA_CONTENT_TYPE;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_STATUS_CODE;
import static org.hypertrace.core.spannormalizer.utils.TestUtils.createKeyValue;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.grpc.Grpc;
import org.hypertrace.core.datamodel.eventfields.rpc.Rpc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RpcFieldsGeneratorTest {
  @Test
  public void testRpcFieldsConverterGrpcSystem() {
    String requestBody = "some grpc request body";
    String responseBody = "some grpc response body";

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), createKeyValue("grpc"));
    tagsMap.put(OTEL_SPAN_TAG_RPC_SERVICE.getValue(), createKeyValue("package.service"));
    tagsMap.put(OTEL_SPAN_TAG_RPC_METHOD.getValue(), createKeyValue("GetId"));

    GrpcFieldsGenerator grpcFieldsGenerator = new GrpcFieldsGenerator();
    RpcFieldsGenerator rpcFieldsGenerator = new RpcFieldsGenerator(grpcFieldsGenerator);
    Event.Builder eventBuilder = Event.newBuilder();
    Rpc.Builder rpcBuilder = rpcFieldsGenerator.getProtocolBuilder(eventBuilder);
    Grpc.Builder grpcBuilder = grpcFieldsGenerator.getProtocolBuilder(eventBuilder);

    Assertions.assertSame(eventBuilder.getRpcBuilder(), rpcBuilder);

    tagsMap.forEach(
        (key, keyValue) -> {
          rpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap);
        });

    tagsMap.put(RPC_ERROR_NAME.getValue(), createKeyValue("Test Error"));
    tagsMap.put(RPC_ERROR_MESSAGE.getValue(), createKeyValue("This error is a test error"));
    tagsMap.put(RPC_STATUS_CODE.getValue(), createKeyValue(1));
    tagsMap.put(RPC_REQUEST_METADATA_X_FORWARDED_FOR.getValue(), createKeyValue("198.12.34.1"));
    tagsMap.put(RPC_REQUEST_METADATA_AUTHORITY.getValue(), createKeyValue("testservice:45"));
    tagsMap.put(RPC_REQUEST_METADATA_CONTENT_TYPE.getValue(), createKeyValue("application/grpc"));
    tagsMap.put(RPC_REQUEST_METADATA_PATH.getValue(), createKeyValue("/package.service/GetId"));
    tagsMap.put(RPC_REQUEST_METADATA_USER_AGENT.getValue(), createKeyValue("grpc-go/1.17.0"));
    tagsMap.put(RPC_RESPONSE_METADATA_CONTENT_TYPE.getValue(), createKeyValue("application/grpc"));
    tagsMap.put(RPC_REQUEST_METADATA.getValue() + ".content-encoding", createKeyValue("identity"));
    tagsMap.put(RPC_RESPONSE_METADATA.getValue() + ".server", createKeyValue("envoy"));

    tagsMap.put(RPC_REQUEST_BODY.getValue(), createKeyValue(requestBody));
    tagsMap.put(RPC_RESPONSE_BODY.getValue(), createKeyValue(responseBody));

    tagsMap.forEach(
        (key, keyValue) -> {
          rpcFieldsGenerator.handleKeyIfNecessary(key, keyValue, eventBuilder, tagsMap);
        });

    Assertions.assertEquals("some grpc request body", grpcBuilder.getRequestBuilder().getBody());
    Assertions.assertEquals("some grpc response body", grpcBuilder.getResponseBuilder().getBody());
    Assertions.assertEquals("Test Error", grpcBuilder.getResponseBuilder().getErrorName());
    Assertions.assertEquals(
        "This error is a test error", grpcBuilder.getResponseBuilder().getErrorMessage());
    Assertions.assertEquals(1, grpcBuilder.getResponseBuilder().getStatusCode());
    Assertions.assertEquals(requestBody.length(), grpcBuilder.getRequestBuilder().getSize());
    Assertions.assertEquals(responseBody.length(), grpcBuilder.getResponseBuilder().getSize());

    Assertions.assertEquals("grpc", eventBuilder.getRpcBuilder().getSystem());
    Assertions.assertEquals("package.service", eventBuilder.getRpcBuilder().getService());
    Assertions.assertEquals("GetId", eventBuilder.getRpcBuilder().getMethod());
    Assertions.assertEquals(
        "testservice:45",
        eventBuilder
            .getGrpcBuilder()
            .getRequestBuilder()
            .getRequestMetadataBuilder()
            .getAuthority());
    Assertions.assertEquals(
        "application/grpc",
        eventBuilder
            .getGrpcBuilder()
            .getRequestBuilder()
            .getRequestMetadataBuilder()
            .getContentType());
    Assertions.assertEquals(
        "/package.service/GetId",
        eventBuilder.getGrpcBuilder().getRequestBuilder().getRequestMetadataBuilder().getPath());
    Assertions.assertEquals(
        "grpc-go/1.17.0",
        eventBuilder
            .getGrpcBuilder()
            .getRequestBuilder()
            .getRequestMetadataBuilder()
            .getUserAgent());
    Assertions.assertEquals(
        "198.12.34.1",
        eventBuilder
            .getGrpcBuilder()
            .getRequestBuilder()
            .getRequestMetadataBuilder()
            .getXForwardedFor());
    Assertions.assertEquals(
        "application/grpc",
        eventBuilder
            .getGrpcBuilder()
            .getResponseBuilder()
            .getResponseMetadataBuilder()
            .getContentType());

    Assertions.assertEquals(
        "identity",
        eventBuilder.getGrpcBuilder().getRequestBuilder().getMetadata().get("content-encoding"));
    Assertions.assertEquals(
        "identity",
        eventBuilder
            .getGrpcBuilder()
            .getRequestBuilder()
            .getRequestMetadataBuilder()
            .getOtherMetadata()
            .get("content-encoding"));

    Assertions.assertEquals(
        "envoy", eventBuilder.getGrpcBuilder().getResponseBuilder().getMetadata().get("server"));
    Assertions.assertEquals(
        "envoy",
        eventBuilder
            .getGrpcBuilder()
            .getResponseBuilder()
            .getResponseMetadataBuilder()
            .getOtherMetadata()
            .get("server"));
  }

  @Test
  public void testRpcFieldsConverterNonGrpcSystem() {
    String requestBody = "some grpc request body";
    String responseBody = "some grpc response body";

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), createKeyValue("wcf"));
    tagsMap.put(OTEL_SPAN_TAG_RPC_SERVICE.getValue(), createKeyValue("package.service"));
    tagsMap.put(OTEL_SPAN_TAG_RPC_METHOD.getValue(), createKeyValue("GetId"));

    GrpcFieldsGenerator grpcFieldsGenerator = new GrpcFieldsGenerator();
    RpcFieldsGenerator rpcFieldsGenerator = new RpcFieldsGenerator(grpcFieldsGenerator);
    Event.Builder eventBuilder = Event.newBuilder();
    Rpc.Builder rpcBuilder = rpcFieldsGenerator.getProtocolBuilder(eventBuilder);
    Grpc.Builder grpcBuilder = grpcFieldsGenerator.getProtocolBuilder(eventBuilder);

    Assertions.assertSame(eventBuilder.getRpcBuilder(), rpcBuilder);

    tagsMap.forEach(
        (key, keyValue) -> {
          rpcFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap);
        });

    tagsMap.put(RPC_ERROR_NAME.getValue(), createKeyValue("Test Error"));
    tagsMap.put(RPC_ERROR_MESSAGE.getValue(), createKeyValue("This error is a test error"));
    tagsMap.put(RPC_STATUS_CODE.getValue(), createKeyValue(1));
    tagsMap.put(RPC_REQUEST_METADATA_X_FORWARDED_FOR.getValue(), createKeyValue("198.12.34.1"));
    tagsMap.put(RPC_REQUEST_METADATA_AUTHORITY.getValue(), createKeyValue("testservice:45"));
    tagsMap.put(RPC_REQUEST_METADATA_CONTENT_TYPE.getValue(), createKeyValue("application/grpc"));
    tagsMap.put(RPC_REQUEST_METADATA_PATH.getValue(), createKeyValue("/package.service/GetId"));
    tagsMap.put(RPC_REQUEST_METADATA_USER_AGENT.getValue(), createKeyValue("grpc-go/1.17.0"));
    tagsMap.put(RPC_RESPONSE_METADATA_CONTENT_TYPE.getValue(), createKeyValue("application/grpc"));
    tagsMap.put(RPC_REQUEST_METADATA.getValue() + ".content-encoding", createKeyValue("identity"));
    tagsMap.put(RPC_RESPONSE_METADATA.getValue() + ".server", createKeyValue("envoy"));

    tagsMap.put(RPC_REQUEST_BODY.getValue(), createKeyValue(requestBody));
    tagsMap.put(RPC_RESPONSE_BODY.getValue(), createKeyValue(responseBody));

    tagsMap.forEach(
        (key, keyValue) -> {
          Assertions.assertEquals(
              false, rpcFieldsGenerator.handleKeyIfNecessary(key, keyValue, eventBuilder, tagsMap));
        });
  }
}
