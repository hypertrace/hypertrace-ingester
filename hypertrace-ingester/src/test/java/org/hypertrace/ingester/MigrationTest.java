package org.hypertrace.ingester;

import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_GRPC_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_RESPONSE_BODY;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_METHOD;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HOST;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_DOT_AGENT;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_UNDERSCORE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_DASH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_QUERY_STRING;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_METHOD;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_URL;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SYSTEM;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_AUTHORITY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_USER_AGENT;

import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.semantic.convention.constants.error.OTelErrorSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.rpc.OTelRpcSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanNormalizer;
import org.hypertrace.semantic.convention.utils.http.HttpSemanticConventionUtils;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MigrationTest {

  private final Random random = new Random();

  @BeforeEach
  public void resetSingleton()
      throws SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException {
    // Since JaegerToRawSpanConverter is a singleton, we need to reset it for unit tests to
    // recreate the instance.
    Field instance = JaegerSpanNormalizer.class.getDeclaredField("INSTANCE");
    instance.setAccessible(true);
    instance.set(null, null);
  }

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

  @Test
  public void testHttpFields() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_METHOD))
                    .setVStr("GET"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD))
                    .setVStr("PUT"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL))
                    .setVStr("https://example.ai/url1"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai/url2"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_URL))
                    .setVStr("https://example.ai/url3"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_HOST))
                    .setVStr("example.ai"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_PATH))
                    .setVStr("/url1"))
            .addTags(
                KeyValue.newBuilder().setKey(RawSpanConstants.getValue(HTTP_PATH)).setVStr("/url2"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_DOT_AGENT))
                    .setVStr("Chrome 1"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE))
                    .setVStr("Chrome 2"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH))
                    .setVStr("Chrome 3"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER))
                    .setVStr("Chrome 4"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT))
                    .setVStr("Chrome 5"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(ENVOY_REQUEST_SIZE))
                    .setVStr("50"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_SIZE))
                    .setVStr("40"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE))
                    .setVStr("30"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_RESPONSE_SIZE))
                    .setVStr("20"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING))
                    .setVStr("a1=v1&a2=v2"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getMethod(),
        HttpSemanticConventionUtils.getHttpMethod(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUrl(),
        HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUserAgent(),
        HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getSize(),
        HttpSemanticConventionUtils.getHttpRequestSize(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getResponse().getSize(),
        HttpSemanticConventionUtils.getHttpResponseSize(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
  }

  @Test
  public void testRequestMethodPriority() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_METHOD))
                    .setVStr("GET"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD))
                    .setVStr("PUT"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getMethod(),
        HttpSemanticConventionUtils.getHttpMethod(rawSpan.getEvent()).get());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD))
                    .setVStr("POST"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getMethod(),
        HttpSemanticConventionUtils.getHttpMethod(rawSpan.getEvent()).get());
  }

  @Test
  public void testRequestUrlTagKeysPriority() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL))
                    .setVStr("https://example.ai/url1"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai/url2"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_URL))
                    .setVStr("https://example.ai/url3"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUrl(),
        HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai/url2"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_URL))
                    .setVStr("https://example.ai/url3"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUrl(),
        HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai/url2"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUrl(),
        HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get());
  }

  @Test
  public void testRequestPathTagKeysPriority() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_PATH))
                    .setVStr("/path1"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_PATH))
                    .setVStr("/path2"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_PATH))
                    .setVStr("/path2"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
  }

  @Test
  public void testInvalidRequestPath() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_PATH))
                    .setVStr("path1"))
            .addTags(
                KeyValue.newBuilder().setKey(RawSpanConstants.getValue(HTTP_PATH)).setVStr("  "))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertFalse(HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).isPresent());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_PATH))
                    .setVStr("path1"))
            .addTags(
                KeyValue.newBuilder().setKey(RawSpanConstants.getValue(HTTP_PATH)).setVStr("/"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals("/", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
  }

  @Test
  public void testRequestUserAgentTagKeysPriority() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_DOT_AGENT))
                    .setVStr("Chrome 1"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE))
                    .setVStr("Chrome 2"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH))
                    .setVStr("Chrome 3"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER))
                    .setVStr("Chrome 4"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT))
                    .setVStr("Chrome 5"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUserAgent(),
        HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE))
                    .setVStr("Chrome 2"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH))
                    .setVStr("Chrome 3"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER))
                    .setVStr("Chrome 4"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT))
                    .setVStr("Chrome 5"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUserAgent(),
        HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH))
                    .setVStr("Chrome 3"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER))
                    .setVStr("Chrome 4"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT))
                    .setVStr("Chrome 5"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUserAgent(),
        HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER))
                    .setVStr("Chrome 4"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT))
                    .setVStr("Chrome 5"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUserAgent(),
        HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_USER_AGENT))
                    .setVStr("Chrome 5"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUserAgent(),
        HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get());
  }

  @Test
  public void testRequestSizeTagKeysPriority() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(ENVOY_REQUEST_SIZE))
                    .setVStr("50"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_SIZE))
                    .setVStr("40"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getSize(),
        HttpSemanticConventionUtils.getHttpRequestSize(rawSpan.getEvent()).get());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_SIZE))
                    .setVStr("35"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getSize(),
        HttpSemanticConventionUtils.getHttpRequestSize(rawSpan.getEvent()).get());
  }

  @Test
  public void testResponseSizeTagKeysPriority() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE))
                    .setVStr("100"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_RESPONSE_SIZE))
                    .setVStr("90"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getResponse().getSize(),
        HttpSemanticConventionUtils.getHttpResponseSize(rawSpan.getEvent()).get());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_RESPONSE_SIZE))
                    .setVStr("85"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getResponse().getSize(),
        HttpSemanticConventionUtils.getHttpResponseSize(rawSpan.getEvent()).get());
  }

  @Test
  public void testRelativeUrlNotSetsUrlField() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("/dispatch/test?a=b&k1=v1"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertFalse(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
  }

  @Test
  public void testAbsoluteUrlNotSetsUrlField() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("http://abc.xyz/dispatch/test?a=b&k1=v1"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUrl(),
        HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get());
    Assertions.assertTrue(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
  }

  @Test
  public void testInvalidUrlRejectedByUrlValidator() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai/apis/5673/events?a1=v1&a2=v2"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
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

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
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

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertFalse(
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // No path
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertFalse(
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // Relative URL - should extract path and query string only
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("/apis/5673/events?a1=v1&a2=v2"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertFalse(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
    Assertions.assertFalse(
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).isPresent());
    Assertions.assertFalse(HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).isPresent());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
    Assertions.assertEquals(
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

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
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

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
  }

  @Test
  public void testHttpFieldGenerationForOtelSpan() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_TARGET.getValue())
                    .setVStr("/url2"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("https://example.ai/url1"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_USER_AGENT.getValue())
                    .setVStr("Chrome 1"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_REQUEST_SIZE.getValue())
                    .setVStr("100"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_RESPONSE_SIZE.getValue())
                    .setVStr("200"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_METHOD.getValue())
                    .setVStr("GET"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_SCHEME.getValue())
                    .setVStr("https"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getMethod(),
        HttpSemanticConventionUtils.getHttpMethod(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUrl(),
        HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUserAgent(),
        HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getSize(),
        HttpSemanticConventionUtils.getHttpRequestSize(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getResponse().getSize(),
        HttpSemanticConventionUtils.getHttpResponseSize(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
  }

  @Test
  public void testPopulateOtherFieldsOTelSpan() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span = Span.newBuilder().build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertFalse(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
    Assertions.assertFalse(
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).isPresent());
    Assertions.assertFalse(HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).isPresent());
    Assertions.assertFalse(HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).isPresent());
    Assertions.assertFalse(
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("https://example.ai/apis/5673/events?a1=v1&a2=v2"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
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

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getQueryString(),
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
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

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    Assertions.assertFalse(
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // No path
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("https://example.ai"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    Assertions.assertFalse(
        HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // Relative URL - should extract path and query string only
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("/apis/5673/events?a1=v1&a2=v2"))
            .build();

    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertFalse(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
    Assertions.assertFalse(
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).isPresent());
    Assertions.assertFalse(HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).isPresent());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
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

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    Assertions.assertEquals(
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

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getHost(),
        HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getScheme(),
        HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    Assertions.assertEquals(
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

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUrl(),
        "http://example.internal.com:50850/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel");

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getUrl(),
        HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get());
  }

  @Test
  public void testIsAbsoluteUrl() {
    Assertions.assertTrue(HttpSemanticConventionUtils.isAbsoluteUrl("http://example.com/abc/xyz"));
    Assertions.assertFalse(HttpSemanticConventionUtils.isAbsoluteUrl("/abc/xyz"));
  }

  @Test
  public void testGetPathFromUrl() {
    Optional<String> path =
        HttpSemanticConventionUtils.getPathFromUrlObject(
            "/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel");
    Assertions.assertEquals(path.get(), "/api/v1/gatekeeper/check");
  }

  @Test
  public void testSetPath() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_TARGET.getValue())
                    .setVStr(
                        "/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getHttp().getRequest().getPath(),
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        "/api/v1/gatekeeper/check",
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
  }

  @Test
  public void testGrpcFields() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(GRPC_ERROR_MESSAGE))
                    .setVStr("Some error message"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_CODE))
                    .setVStr("12"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(GRPC_STATUS_CODE))
                    .setVStr("13"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE))
                    .setVStr("14"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE))
                    .setVStr("CENSUS_RESPONSE_STATUS_MESSAGE"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE))
                    .setVStr("ENVOY_GRPC_STATUS_MESSAGE"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(GRPC_REQUEST_BODY))
                    .setVStr("some grpc request body"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(GRPC_RESPONSE_BODY))
                    .setVStr("some grpc response body"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getErrorMessage(),
        RpcSemanticConventionUtils.getGrpcErrorMsg(rawSpan.getEvent()));
    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getStatusCode(),
        RpcSemanticConventionUtils.getGrpcStatusCode(rawSpan.getEvent()));
    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getStatusMessage(),
        RpcSemanticConventionUtils.getGrpcStatusMsg(rawSpan.getEvent()));
    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getSize(),
        RpcSemanticConventionUtils.getGrpcResponseSize(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getRequest().getSize(),
        RpcSemanticConventionUtils.getGrpcRequestSize(rawSpan.getEvent()).get());
  }

  @Test
  public void testGrpcFieldsConverterEnvoyRequestAndResponseSizeHigherPriority() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(GRPC_REQUEST_BODY))
                    .setVStr("some grpc request body"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(GRPC_RESPONSE_BODY))
                    .setVStr("some grpc response body"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(ENVOY_REQUEST_SIZE))
                    .setVStr("200"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE))
                    .setVStr("400"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getSize(),
        RpcSemanticConventionUtils.getGrpcResponseSize(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getRequest().getSize(),
        RpcSemanticConventionUtils.getGrpcRequestSize(rawSpan.getEvent()).get());
  }

  @Test
  public void testGrpcFieldsConverterStatusCodePriority() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_CODE))
                    .setVStr("12"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(GRPC_STATUS_CODE))
                    .setVStr("13"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE))
                    .setVStr("14"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getStatusCode(),
        RpcSemanticConventionUtils.getGrpcStatusCode(rawSpan.getEvent()));
    Assertions.assertEquals(12, rawSpan.getEvent().getGrpc().getResponse().getStatusCode());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(GRPC_STATUS_CODE))
                    .setVStr("13"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE))
                    .setVStr("14"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getStatusCode(),
        RpcSemanticConventionUtils.getGrpcStatusCode(rawSpan.getEvent()));
    Assertions.assertEquals(13, rawSpan.getEvent().getGrpc().getResponse().getStatusCode());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE))
                    .setVStr("14"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getStatusCode(),
        RpcSemanticConventionUtils.getGrpcStatusCode(rawSpan.getEvent()));
    Assertions.assertEquals(14, rawSpan.getEvent().getGrpc().getResponse().getStatusCode());
  }

  @Test
  public void testGrpcFieldsConverterStatusMessagePriority() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE))
                    .setVStr("CENSUS_RESPONSE_STATUS_MESSAGE"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE))
                    .setVStr("ENVOY_GRPC_STATUS_MESSAGE"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getStatusMessage(),
        RpcSemanticConventionUtils.getGrpcStatusMsg(rawSpan.getEvent()));
    Assertions.assertEquals(
        "CENSUS_RESPONSE_STATUS_MESSAGE",
        rawSpan.getEvent().getGrpc().getResponse().getStatusMessage());

    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE))
                    .setVStr("ENVOY_GRPC_STATUS_MESSAGE"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getStatusMessage(),
        RpcSemanticConventionUtils.getGrpcStatusMsg(rawSpan.getEvent()));
    Assertions.assertEquals(
        "ENVOY_GRPC_STATUS_MESSAGE", rawSpan.getEvent().getGrpc().getResponse().getStatusMessage());
  }

  @Test
  public void testGrpcFieldsForOTelSpan() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelRpcSemanticConventions.GRPC_STATUS_CODE.getValue())
                    .setVStr("5"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getStatusCode(),
        RpcSemanticConventionUtils.getGrpcStatusCode(rawSpan.getEvent()));
  }

  @Test
  public void testPopulateOtherFields() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelErrorSemanticConventions.EXCEPTION_MESSAGE.getValue())
                    .setVStr("resource not found"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelRpcSemanticConventions.RPC_SYSTEM.getValue())
                    .setVStr(OTelRpcSemanticConventions.RPC_SYSTEM_VALUE_GRPC.getValue()))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getResponse().getErrorMessage(),
        RpcSemanticConventionUtils.getGrpcErrorMsg(rawSpan.getEvent()));
  }

  @Test
  public void testRpcFieldsGrpcSystem() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder().setKey(OTEL_SPAN_TAG_RPC_SYSTEM.getValue()).setVStr("grpc"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RPC_REQUEST_METADATA_AUTHORITY.getValue())
                    .setVStr("testservice:45"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RPC_REQUEST_METADATA_USER_AGENT.getValue())
                    .setVStr("grpc-go/1.17.0"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getRequest().getRequestMetadata().getAuthority(),
        RpcSemanticConventionUtils.getGrpcAuthority(rawSpan.getEvent()).get());
    Assertions.assertEquals(
        rawSpan.getEvent().getGrpc().getRequest().getRequestMetadata().getUserAgent(),
        RpcSemanticConventionUtils.getGrpcUserAgent(rawSpan.getEvent()).get());
  }

  @Test
  public void testRpcFieldsNonGrpcSystem() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));

    Span span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder().setKey(OTEL_SPAN_TAG_RPC_SYSTEM.getValue()).setVStr("wcf"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RPC_REQUEST_METADATA_AUTHORITY.getValue())
                    .setVStr("testservice:45"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RPC_REQUEST_METADATA_USER_AGENT.getValue())
                    .setVStr("grpc-go/1.17.0"))
            .build();
    RawSpan rawSpan = normalizer.convert("tenant-key", span);

    Assertions.assertFalse(
        RpcSemanticConventionUtils.getGrpcAuthority(rawSpan.getEvent()).isPresent());
    Assertions.assertFalse(
        RpcSemanticConventionUtils.getGrpcUserAgent(rawSpan.getEvent()).isPresent());
  }
}
