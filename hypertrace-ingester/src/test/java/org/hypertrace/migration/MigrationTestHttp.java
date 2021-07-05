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
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HOST;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_METHOD;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_QUERY_STRING;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_DASH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_UNDERSCORE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_DOT_AGENT;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_METHOD;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_URL;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SYSTEM;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_AUTHORITY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_USER_AGENT;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.semantic.convention.constants.error.OTelErrorSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.rpc.OTelRpcSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanNormalizer;
import org.hypertrace.semantic.convention.utils.http.HttpSemanticConventionUtils;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class MigrationTestHttp {

  private final Random random = new Random();
  private JaegerSpanNormalizer normalizer;

  private Map<String, Object> getCommonConfig() {
    return Map.of(
        "span.type",
        "jaeger",
        "input.topic",
        "jaeger-spans",
        "output.topic",
        "raw-spans-from-jaeger-spans",
        "kafka.streams.config",
        Map.of(
            "application.id",
            "jaeger-spans-to-raw-spans-job",
            "bootstrap.servers",
            "localhost:9092"),
        "schema.registry.config",
        Map.of("schema.registry.url", "http://localhost:8081"));
  }

  @BeforeEach
  public void setup()
      throws SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException {
    // Since JaegerToRawSpanConverter is a singleton, we need to reset it for unit tests to
    // recreate the instance.
    Field instance = JaegerSpanNormalizer.class.getDeclaredField("INSTANCE");
    instance.setAccessible(true);
    instance.set(null, null);

    // Create a JaegerSpanNormaliser
    String tenantId = "tenant-" + this.random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    this.normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));
  }

  @Test
  public void testHttpFields() throws Exception {

    Map<String, String> tagsMap =
        new HashMap<>() {
          {
            put(RawSpanConstants.getValue(HTTP_REQUEST_METHOD), "GET");
            put(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD), "PUT");
            put(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL), "https://example.ai/url1");
            put(RawSpanConstants.getValue(HTTP_URL), "https://example.ai/url2");
            put(RawSpanConstants.getValue(HTTP_REQUEST_URL), "https://example.ai/url3");
            put(RawSpanConstants.getValue(HTTP_HOST), "example.ai");
            put(RawSpanConstants.getValue(HTTP_REQUEST_PATH), "/url1");
            put(RawSpanConstants.getValue(HTTP_PATH), "/url2");
            put(RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), "Chrome 1");
            put(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE), "Chrome 2");
            put(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH), "Chrome 3");
            put(RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), "Chrome 4");
            put(RawSpanConstants.getValue(HTTP_USER_AGENT), "Chrome 5");
            put(RawSpanConstants.getValue(ENVOY_REQUEST_SIZE), "50");
            put(RawSpanConstants.getValue(HTTP_REQUEST_SIZE), "40");
            put(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE), "30");
            put(RawSpanConstants.getValue(HTTP_RESPONSE_SIZE), "20");
            put(RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING), "a1=v1&a2=v2");
          }
        };

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertAll(
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getMethod(),
                HttpSemanticConventionUtils.getHttpMethod(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getUrl(),
                HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getHost(),
                HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getPath(),
                HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getUserAgent(),
                HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getSize(),
                HttpSemanticConventionUtils.getHttpRequestSize(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getResponse().getSize(),
                HttpSemanticConventionUtils.getHttpResponseSize(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getQueryString(),
                HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get()));
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingRequestMethodPriority")
  public void testRequestMethodPriority(Map<String, String> tagsMap) throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getMethod(),
        HttpSemanticConventionUtils.getHttpMethod(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingRequestUrlTagKeysPriority")
  public void testRequestUrlTagKeysPriority(Map<String, String> tagsMap) throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUrl(),
        HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingRequestPathTagKeysPriority")
  public void testRequestPathTagKeysPriority(Map<String, String> tagsMap) throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
  }

  @Test
  public void testInvalidRequestPath() throws Exception {

    Span span =
        createSpanFromTags(
            Map.of(
                RawSpanConstants.getValue(HTTP_REQUEST_PATH), "path1",
                RawSpanConstants.getValue(HTTP_PATH), "  "));
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertFalse(HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).isPresent());

    span =
        createSpanFromTags(
            Map.of(
                RawSpanConstants.getValue(HTTP_REQUEST_PATH), "path1",
                RawSpanConstants.getValue(HTTP_PATH), "/"));
    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals("/", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingRequestUserAgentTagKeysPriority")
  public void testRequestUserAgentTagKeysPriority(Map<String, String> tagsMap) throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUserAgent(),
        HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingRequestSizeTagKeysPriority")
  public void testRequestSizeTagKeysPriority(Map<String, String> tagsMap) throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getSize(),
        HttpSemanticConventionUtils.getHttpRequestSize(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingResponseSizeTagKeysPriority")
  public void testResponseSizeTagKeysPriority(Map<String, String> tagsMap) throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getResponse().getSize(),
        HttpSemanticConventionUtils.getHttpResponseSize(rawSpan.getEvent()).get());
  }

  @Test
  public void testRelativeUrlNotSetsUrlField() throws Exception {

    Span span =
        createSpanFromTags(Map.of(RawSpanConstants.getValue(HTTP_URL), "/dispatch/test?a=b&k1=v1"));
    RawSpan rawSpan = normalizer.convert("tenant-key", span);
    assertFalse(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
  }

  @Test
  public void testAbsoluteUrlNotSetsUrlField() throws Exception {

    Span span =
        createSpanFromTags(
            Map.of(RawSpanConstants.getValue(HTTP_URL), "http://abc.xyz/dispatch/test?a=b&k1=v1"));
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertAll(
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getUrl(),
                HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get()),
        () -> assertTrue(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent()));
  }

  @Test
  public void testInvalidUrlRejectedByUrlValidator() throws Exception {
    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai/apis/5673/events?a1=v1&a2=v2"))
            .build();

    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());

    // Removes the trailing "/" for path
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai/apis/5673/events/?a1=v1&a2=v2"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());

    // No query
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai/apis/5673/events"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertFalse(HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // No path
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertFalse(HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // Relative URL - should extract path and query string only
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("/apis/5673/events?a1=v1&a2=v2"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertFalse(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).isPresent());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());

    // "/" home path, host with port
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("http://example.ai:9000/?a1=v1&a2=v2"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());

    // Set path and query string before calling populateOtherFields. Simulate case where fields came
    // from attributes
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("http://example.ai:9000/apis/4533?a1=v1&a2=v2"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING))
                    .setVStr("some-query-str=v1"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_PATH))
                    .setVStr("/some-test-path"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
  }

  @Test
  public void testHttpFieldGenerationForOtelSpan() throws Exception {

    Map<String, String> tagsMap =
        new HashMap<>() {
          {
            put(OTelHttpSemanticConventions.HTTP_TARGET.getValue(), "/url2");
            put(OTelHttpSemanticConventions.HTTP_URL.getValue(), "https://example.ai/url1");
            put(OTelHttpSemanticConventions.HTTP_USER_AGENT.getValue(), "Chrome 1");
            put(OTelHttpSemanticConventions.HTTP_REQUEST_SIZE.getValue(), "100");
            put(OTelHttpSemanticConventions.HTTP_RESPONSE_SIZE.getValue(), "200");
            put(OTelHttpSemanticConventions.HTTP_METHOD.getValue(), "GET");
            put(OTelHttpSemanticConventions.HTTP_SCHEME.getValue(), "https");
          }
        };

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertAll(
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getMethod(),
                HttpSemanticConventionUtils.getHttpMethod(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getUrl(),
                HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getPath(),
                HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getUserAgent(),
                HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getSize(),
                HttpSemanticConventionUtils.getHttpRequestSize(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getResponse().getSize(),
                HttpSemanticConventionUtils.getHttpResponseSize(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getScheme(),
                HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get()));
  }

  @Test
  public void testPopulateOtherFieldsOTelSpan() throws Exception {
    Span span = Span.newBuilder().build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertFalse(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("https://example.ai/apis/5673/events?a1=v1&a2=v2"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());

    // Removes the trailing "/" for path
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("https://example.ai/apis/5673/events/?a1=v1&a2=v2"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());

    // No query
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("https://example.ai/apis/5673/events"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertFalse(HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // No path
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("https://example.ai"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertFalse(HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // Relative URL - should extract path and query string only
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("/apis/5673/events?a1=v1&a2=v2"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertFalse(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).isPresent());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());

    // "/" home path, host with port
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("http://example.ai:9000/?a1=v1&a2=v2"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());

    // Set path and query string before calling populateOtherFields. Simulate case where fields came
    // from attributes
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("http://example.ai:9000/apis/4533?a1=v1&a2=v2"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING))
                    .setVStr("some-query-str=v1"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_TARGET.getValue())
                    .setVStr("/some-test-path"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());

    // originally set url is a relative url, should be overridden
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_SCHEME.getValue())
                    .setVStr("http"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelSpanSemanticConventions.SPAN_KIND.getValue())
                    .setVStr("server"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_NET_HOST_NAME.getValue())
                    .setVStr("example.internal.com"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_NET_HOST_PORT.getValue())
                    .setVStr("50850"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_TARGET.getValue())
                    .setVStr(
                        "/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUrl(),
        "http://example.internal.com:50850/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel");

    assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUrl(),
        HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get());
  }

  @Test
  public void testSetPath() throws Exception {

    Span span =
        createSpanFromTags(
            Map.of(
                OTelHttpSemanticConventions.HTTP_TARGET.getValue(),
                "/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel"));
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertAll(
        () ->
            assertEquals(
                rawSpan.getEvent().getHttp().getRequest().getPath(),
                HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                "/api/v1/gatekeeper/check",
                HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get()));
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
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getErrorMessage(),
        RpcSemanticConventionUtils.getGrpcErrorMsg(rawSpan.getEvent()));
  }

  private static Stream<Map<String, String>> provideMapForTestingRequestMethodPriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(HTTP_REQUEST_METHOD), "GET",
            RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD), "PUT");

    Map<String, String> tagsMap2 =
        Map.of(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD), "POST");

    return Stream.of(tagsMap1, tagsMap2);
  }

  private static Stream<Map<String, String>> provideMapForTestingRequestUrlTagKeysPriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL), "https://example.ai/url1",
            RawSpanConstants.getValue(HTTP_URL), "https://example.ai/url2",
            RawSpanConstants.getValue(HTTP_REQUEST_URL), "https://example.ai/url3");

    Map<String, String> tagsMap2 =
        Map.of(
            RawSpanConstants.getValue(HTTP_URL), "https://example.ai/url2",
            RawSpanConstants.getValue(HTTP_REQUEST_URL), "https://example.ai/url3");

    Map<String, String> tagsMap3 =
        Map.of(RawSpanConstants.getValue(HTTP_URL), "https://example.ai/url2");

    return Stream.of(tagsMap1, tagsMap2, tagsMap3);
  }

  private static Stream<Map<String, String>> provideMapForTestingRequestPathTagKeysPriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(HTTP_REQUEST_PATH), "/path1",
            RawSpanConstants.getValue(HTTP_PATH), "/path2");

    Map<String, String> tagsMap2 = Map.of(RawSpanConstants.getValue(HTTP_PATH), "/path2");

    return Stream.of(tagsMap1, tagsMap2);
  }

  private static Stream<Map<String, String>> provideMapForTestingRequestUserAgentTagKeysPriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), "Chrome 1",
            RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE), "Chrome 2",
            RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH), "Chrome 3",
            RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), "Chrome 4",
            RawSpanConstants.getValue(HTTP_USER_AGENT), "Chrome 5");

    Map<String, String> tagsMap2 =
        Map.of(
            RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE), "Chrome 2",
            RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH), "Chrome 3",
            RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), "Chrome 4",
            RawSpanConstants.getValue(HTTP_USER_AGENT), "Chrome 5");

    Map<String, String> tagsMap3 =
        Map.of(
            RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH), "Chrome 3",
            RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), "Chrome 4",
            RawSpanConstants.getValue(HTTP_USER_AGENT), "Chrome 5");

    Map<String, String> tagsMap4 =
        Map.of(
            RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), "Chrome 4",
            RawSpanConstants.getValue(HTTP_USER_AGENT), "Chrome 5");

    Map<String, String> tagsMap5 = Map.of(RawSpanConstants.getValue(HTTP_USER_AGENT), "Chrome 5");

    return Stream.of(tagsMap1, tagsMap2, tagsMap3, tagsMap4, tagsMap5);
  }

  private static Stream<Map<String, String>> provideMapForTestingRequestSizeTagKeysPriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(ENVOY_REQUEST_SIZE), "50",
            RawSpanConstants.getValue(HTTP_REQUEST_SIZE), "40");

    Map<String, String> tagsMap2 = Map.of(RawSpanConstants.getValue(HTTP_REQUEST_SIZE), "35");

    return Stream.of(tagsMap1, tagsMap2);
  }

  private static Stream<Map<String, String>> provideMapForTestingResponseSizeTagKeysPriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE), "100",
            RawSpanConstants.getValue(HTTP_RESPONSE_SIZE), "90");

    Map<String, String> tagsMap2 = Map.of(RawSpanConstants.getValue(HTTP_RESPONSE_SIZE), "85");

    return Stream.of(tagsMap1, tagsMap2);
  }

  static Span createSpanFromTags(Map<String, String> tagsMap) {
    return Span.newBuilder()
        .addAllTags(
            tagsMap.entrySet().stream()
                .map(
                    entry ->
                        KeyValue.newBuilder()
                            .setKey(entry.getKey())
                            .setVStr(entry.getValue())
                            .build())
                .collect(Collectors.toList()))
        .build();
  }
}
