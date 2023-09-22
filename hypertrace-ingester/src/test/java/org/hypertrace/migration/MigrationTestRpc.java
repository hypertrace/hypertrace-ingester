package org.hypertrace.migration;

import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_GRPC_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_STATUS_CODE;
import static org.hypertrace.core.span.normalizer.constants.OTelRpcSystem.OTEL_RPC_SYSTEM_GRPC;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SYSTEM;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_AUTHORITY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_USER_AGENT;
import static org.hypertrace.core.spannormalizer.util.EventBuilder.buildEvent;
import static org.hypertrace.migration.MigrationTestHttp.createSpanFromTags;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.semantic.convention.constants.error.OTelErrorSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.rpc.OTelRpcSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanNormalizer;
import org.hypertrace.core.spannormalizer.jaeger.ServiceNamer;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class MigrationTestRpc {

  private final Random random = new Random();
  private JaegerSpanNormalizer normalizer;

  @BeforeEach
  public void setup()
      throws SecurityException,
          NoSuchFieldException,
          IllegalArgumentException,
          IllegalAccessException {
    // Since JaegerToRawSpanConverter is a singleton, we need to reset it for unit tests to
    // recreate the instance.
    Field instance = JaegerSpanNormalizer.class.getDeclaredField("INSTANCE");
    instance.setAccessible(true);
    instance.set(null, null);

    // Create a JaegerSpanNormaliser
    String tenantId = "tenant-" + this.random.nextLong();
    Map<String, Object> configs = Map.of("processor", Map.of("defaultTenantId", tenantId));
    this.normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));
  }

  @Test
  public void testGrpcFields() throws Exception {

    String grpcRequestBodyValue = "some grpc request body";
    String grpcResponseBodyValue = "some grpc response body";
    String grpcErrorMessageValue = "some error message";
    String censusResponseStatusMessageValue = "CENSUS_RESPONSE_STATUS_MESSAGE";
    String envoyGrpcStatusMessage = "ENVOY_GRPC_STATUS_MESSAGE";

    Map<String, String> tagsMap =
        new HashMap<>() {
          {
            put(RawSpanConstants.getValue(GRPC_ERROR_MESSAGE), grpcErrorMessageValue);
            put(RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_CODE), "12");
            put(RawSpanConstants.getValue(GRPC_STATUS_CODE), "13");
            put(RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE), "14");
            put(
                RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE),
                censusResponseStatusMessageValue);
            put(RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE), envoyGrpcStatusMessage);
            put(RawSpanConstants.getValue(GRPC_REQUEST_BODY), grpcRequestBodyValue);
            put(RawSpanConstants.getValue(GRPC_RESPONSE_BODY), grpcResponseBodyValue);
          }
        };

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getGrpc());

    assertEquals(
        grpcErrorMessageValue, RpcSemanticConventionUtils.getGrpcErrorMsg(rawSpan.getEvent()));

    assertEquals(12, RpcSemanticConventionUtils.getGrpcStatusCode(rawSpan.getEvent()));

    assertEquals(
        censusResponseStatusMessageValue,
        RpcSemanticConventionUtils.getGrpcStatusMsg(rawSpan.getEvent()));

    assertEquals(
        grpcResponseBodyValue.length(),
        RpcSemanticConventionUtils.getGrpcResponseSize(rawSpan.getEvent()).get());

    assertEquals(
        grpcRequestBodyValue.length(),
        RpcSemanticConventionUtils.getGrpcRequestSize(rawSpan.getEvent()).get());
  }

  @Test
  public void testGrpcFieldsConverterEnvoyRequestAndResponseSizeHigherPriority() throws Exception {

    Map<String, String> tagsMap =
        Map.of(
            RawSpanConstants.getValue(GRPC_REQUEST_BODY), "some grpc request body",
            RawSpanConstants.getValue(GRPC_RESPONSE_BODY), "some grpc response body",
            RawSpanConstants.getValue(ENVOY_REQUEST_SIZE), "200",
            RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE), "400");

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getGrpc());

    assertEquals(400, RpcSemanticConventionUtils.getGrpcResponseSize(rawSpan.getEvent()).get());

    assertEquals(200, RpcSemanticConventionUtils.getGrpcRequestSize(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideArgumentsForTestingGrpcFieldsConverterStatusCodePriority")
  public void testGrpcFieldsConverterStatusCodePriority(Map<String, String> tagsMap, int statusCode)
      throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getGrpc());

    assertAll(
        () ->
            assertEquals(
                statusCode, RpcSemanticConventionUtils.getGrpcStatusCode(rawSpan.getEvent())));
  }

  @Test
  public void testGetGrpcUserAgent() throws Exception {
    String rpcRequestMetadataUserAgentValue = "rpc user agent";
    Map<String, String> tagsMap =
        Map.of(RPC_REQUEST_METADATA_USER_AGENT.getValue(), rpcRequestMetadataUserAgentValue);

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    assertNull(rawSpan.getEvent().getGrpc());
    assertTrue(RpcSemanticConventionUtils.getGrpcUserAgent(rawSpan.getEvent()).isEmpty());

    tagsMap =
        Map.of(
            RPC_REQUEST_METADATA_USER_AGENT.getValue(),
            rpcRequestMetadataUserAgentValue,
            OTEL_SPAN_TAG_RPC_SYSTEM.getValue(),
            OTEL_RPC_SYSTEM_GRPC.getValue());

    span = createSpanFromTags(tagsMap);
    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));
    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getGrpc());
    assertEquals(
        rpcRequestMetadataUserAgentValue,
        RpcSemanticConventionUtils.getGrpcUserAgent(rawSpan.getEvent()).get());
  }

  @Test
  public void testGetGrpcAuthority() throws Exception {
    String rpcRequestMetadataAuthorityValue = "grpc authority";
    Map<String, String> tagsMap =
        Map.of(RPC_REQUEST_METADATA_AUTHORITY.getValue(), rpcRequestMetadataAuthorityValue);

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    assertNull(rawSpan.getEvent().getGrpc());
    assertTrue(RpcSemanticConventionUtils.getGrpcAuthority(rawSpan.getEvent()).isEmpty());

    tagsMap =
        Map.of(
            RPC_REQUEST_METADATA_AUTHORITY.getValue(),
            rpcRequestMetadataAuthorityValue,
            OTEL_SPAN_TAG_RPC_SYSTEM.getValue(),
            OTEL_RPC_SYSTEM_GRPC.getValue());

    span = createSpanFromTags(tagsMap);
    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getGrpc());
    assertEquals(
        rpcRequestMetadataAuthorityValue,
        RpcSemanticConventionUtils.getGrpcAuthority(rawSpan.getEvent()).get());
  }

  private static Stream<Arguments>
      provideArgumentsForTestingGrpcFieldsConverterStatusCodePriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_CODE), "12",
            RawSpanConstants.getValue(GRPC_STATUS_CODE), "13",
            RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE), "14");

    Map<String, String> tagsMap2 =
        Map.of(
            RawSpanConstants.getValue(GRPC_STATUS_CODE), "13",
            RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE), "14");

    Map<String, String> tagsMap3 =
        Map.of(RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE), "14");

    return Stream.of(
        Arguments.arguments(tagsMap1, 12),
        Arguments.arguments(tagsMap2, 13),
        Arguments.arguments(tagsMap3, 14));
  }

  @ParameterizedTest
  @MethodSource("provideArgumentsForTestingGrpcFieldsConverterStatusMessagePriority")
  public void testGrpcFieldsConverterStatusMessagePriority(
      Map<String, String> tagsMap, String statusMessage) throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getGrpc());
    assertEquals(statusMessage, RpcSemanticConventionUtils.getGrpcStatusMsg(rawSpan.getEvent()));
  }

  private static Stream<Arguments>
      provideArgumentsForTestingGrpcFieldsConverterStatusMessagePriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE),
            "CENSUS_RESPONSE_STATUS_MESSAGE",
            RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE),
            "ENVOY_GRPC_STATUS_MESSAGE");

    Map<String, String> tagsMap2 =
        Map.of(RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE), "ENVOY_GRPC_STATUS_MESSAGE");

    return Stream.of(
        Arguments.arguments(tagsMap1, "CENSUS_RESPONSE_STATUS_MESSAGE"),
        Arguments.arguments(tagsMap2, "ENVOY_GRPC_STATUS_MESSAGE"));
  }

  @Test
  public void testPopulateOtherFields() throws Exception {

    Map<String, String> tagMap =
        Map.of(
            OTelErrorSemanticConventions.EXCEPTION_MESSAGE.getValue(),
            "resource not found",
            OTelRpcSemanticConventions.RPC_SYSTEM.getValue(),
            OTelRpcSemanticConventions.RPC_SYSTEM_VALUE_GRPC.getValue());

    Span span = createSpanFromTags(tagMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    assertEquals(
        "resource not found", RpcSemanticConventionUtils.getGrpcErrorMsg(rawSpan.getEvent()));
  }

  @Test
  public void testGrpcFieldsForOTelSpan() throws Exception {

    Map<String, String> tagMap =
        Map.of(OTelRpcSemanticConventions.GRPC_STATUS_CODE.getValue(), "5");

    Span span = createSpanFromTags(tagMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getGrpc());

    assertEquals(5, RpcSemanticConventionUtils.getGrpcStatusCode(rawSpan.getEvent()));
  }

  @Test
  public void testRpcFieldsGrpcSystem() throws Exception {

    Map<String, String> tagMap =
        Map.of(
            OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), "grpc",
            RPC_REQUEST_METADATA_AUTHORITY.getValue(), "testservice:45",
            RPC_REQUEST_METADATA_USER_AGENT.getValue(), "grpc-go/1.17.0");

    Span span = createSpanFromTags(tagMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getGrpc());

    assertAll(
        () ->
            assertEquals(
                "testservice:45",
                RpcSemanticConventionUtils.getGrpcAuthority(rawSpan.getEvent()).get()));
  }

  @Test
  public void testRpcFieldsNonGrpcSystem() throws Exception {

    Map<String, String> tagsMap =
        Map.of(
            OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), "wcf",
            RPC_REQUEST_METADATA_AUTHORITY.getValue(), "testservice:45",
            RPC_REQUEST_METADATA_USER_AGENT.getValue(), "grpc-go/1.17.0");

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    assertAll(
        () ->
            assertFalse(
                RpcSemanticConventionUtils.getGrpcAuthority(rawSpan.getEvent()).isPresent()),
        () ->
            assertFalse(
                RpcSemanticConventionUtils.getGrpcUserAgent(rawSpan.getEvent()).isPresent()));
  }
}
