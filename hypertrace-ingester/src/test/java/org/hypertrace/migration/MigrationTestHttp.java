package org.hypertrace.migration;

import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HOST;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_METHOD;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_QUERY_STRING;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_DASH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_UNDERSCORE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_DOT_AGENT;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_METHOD;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_URL;
import static org.hypertrace.core.spannormalizer.util.EventBuilder.buildEvent;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanNormalizer;
import org.hypertrace.core.spannormalizer.jaeger.ServiceNamer;
import org.hypertrace.semantic.convention.utils.http.HttpSemanticConventionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class MigrationTestHttp {

  private final Random random = new Random();
  private JaegerSpanNormalizer normalizer;

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
    Map<String, Object> configs = Map.of("processor", Map.of("defaultTenantId", tenantId));
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
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertAll(
        () ->
            assertEquals(
                "GET", HttpSemanticConventionUtils.getHttpMethod(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                "https://example.ai/url1",
                HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                "example.ai", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                "/url1", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                "Chrome 1", HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                50, HttpSemanticConventionUtils.getHttpRequestSize(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                30, HttpSemanticConventionUtils.getHttpResponseSize(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                "a1=v1&a2=v2",
                HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get()));
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingRequestMethodPriority")
  public void testRequestMethodPriority(Map<String, String> tagsMap, String expectedMethod)
      throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals(
        expectedMethod, HttpSemanticConventionUtils.getHttpMethod(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingRequestUrlTagKeysPriority")
  public void testRequestUrlTagKeysPriority(Map<String, String> tagsMap, String expectedUrl)
      throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals(expectedUrl, HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingRequestPathTagKeysPriority")
  public void testRequestPathTagKeysPriority(Map<String, String> tagsMap, String expectedPath)
      throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals(expectedPath, HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingResponseStatusCodePriority")
  public void testResponseStatusCodePriority(Map<String, String> tagsMap, int statusCode)
      throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals(
        statusCode, HttpSemanticConventionUtils.getHttpResponseStatusCode(rawSpan.getEvent()));
  }

  @Test
  public void testInvalidRequestPath() throws Exception {

    Span span =
        createSpanFromTags(
            Map.of(
                RawSpanConstants.getValue(HTTP_REQUEST_PATH), "path1",
                RawSpanConstants.getValue(HTTP_PATH), "  "));
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    assertFalse(HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).isPresent());

    span =
        createSpanFromTags(
            Map.of(
                RawSpanConstants.getValue(HTTP_REQUEST_PATH), "/path1",
                RawSpanConstants.getValue(HTTP_PATH), "/"));
    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    assertEquals("/path1", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingRequestUserAgentTagKeysPriority")
  public void testRequestUserAgentTagKeysPriority(Map<String, String> tagsMap, String userAgent)
      throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals(userAgent, HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingRequestSizeTagKeysPriority")
  public void testRequestSizeTagKeysPriority(Map<String, String> tagsMap, int requestSize)
      throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals(
        requestSize, HttpSemanticConventionUtils.getHttpRequestSize(rawSpan.getEvent()).get());
  }

  @ParameterizedTest
  @MethodSource("provideMapForTestingResponseSizeTagKeysPriority")
  public void testResponseSizeTagKeysPriority(Map<String, String> tagsMap, int responseSize)
      throws Exception {

    Span span = createSpanFromTags(tagsMap);
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals(
        responseSize, HttpSemanticConventionUtils.getHttpResponseSize(rawSpan.getEvent()).get());
  }

  @Test
  public void testRelativeUrlNotSetsUrlField() throws Exception {

    Span span =
        createSpanFromTags(Map.of(RawSpanConstants.getValue(HTTP_URL), "/dispatch/test?a=b&k1=v1"));
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));
    assertFalse(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
  }

  @Test
  public void testAbsoluteUrlNotSetsUrlField() throws Exception {

    Span span =
        createSpanFromTags(
            Map.of(RawSpanConstants.getValue(HTTP_URL), "http://abc.xyz/dispatch/test?a=b&k1=v1"));
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertAll(
        () -> assertTrue(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent()),
        () ->
            assertEquals(
                "http://abc.xyz/dispatch/test?a=b&k1=v1",
                HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get()));
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

    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals("https", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals("example.ai", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        "/apis/5673/events", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        "a1=v1&a2=v2", HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());

    // Removes the trailing "/" for path
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai/apis/5673/events/?a1=v1&a2=v2"))
            .build();

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals("https", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals("example.ai", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        "/apis/5673/events", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        "a1=v1&a2=v2", HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());

    // No query
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai/apis/5673/events"))
            .build();

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals("https", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals("example.ai", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        "/apis/5673/events", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertFalse(HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // No path
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("https://example.ai"))
            .build();

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals("https", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals("example.ai", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals("/", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertFalse(HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // Relative URL - should extract path and query string only
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("/apis/5673/events?a1=v1&a2=v2"))
            .build();

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertFalse(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).isPresent());
    assertEquals(
        "a1=v1&a2=v2", HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
    assertEquals(
        "/apis/5673/events", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());

    // "/" home path, host with port
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_URL))
                    .setVStr("http://example.ai:9000/?a1=v1&a2=v2"))
            .build();

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals("http", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        "example.ai:9000", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals("/", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        "a1=v1&a2=v2", HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());

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

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals("http", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        "example.ai:9000", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        "/some-test-path", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        "some-query-str=v1",
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
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertAll(
        () ->
            assertEquals(
                "GET", HttpSemanticConventionUtils.getHttpMethod(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                "https://example.ai/url1",
                HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                "/url2", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                "Chrome 1", HttpSemanticConventionUtils.getHttpUserAgent(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                100, HttpSemanticConventionUtils.getHttpRequestSize(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                200, HttpSemanticConventionUtils.getHttpResponseSize(rawSpan.getEvent()).get()),
        () ->
            assertEquals(
                "https", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get()));
  }

  @Test
  public void testPopulateOtherFieldsOTelSpan() throws Exception {
    Span span = Span.newBuilder().build();
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

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

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals("example.ai", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        "a1=v1&a2=v2", HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
    assertEquals(
        "/apis/5673/events", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals("https", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());

    // Removes the trailing "/" for path
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("https://example.ai/apis/5673/events/?a1=v1&a2=v2"))
            .build();

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));
    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals("example.ai", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        "a1=v1&a2=v2", HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());
    assertEquals(
        "/apis/5673/events", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals("https", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());

    // No query
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("https://example.ai/apis/5673/events"))
            .build();

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));

    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals("example.ai", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        "/apis/5673/events", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals("https", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertFalse(HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // No path
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("https://example.ai"))
            .build();

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));
    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals("example.ai", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals("/", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals("https", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertFalse(HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).isPresent());

    // Relative URL - should extract path and query string only
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("/apis/5673/events?a1=v1&a2=v2"))
            .build();

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));
    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertFalse(HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).isPresent());
    assertFalse(HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).isPresent());
    assertEquals(
        "/apis/5673/events", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals(
        "a1=v1&a2=v2", HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());

    // "/" home path, host with port
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(OTelHttpSemanticConventions.HTTP_URL.getValue())
                    .setVStr("http://example.ai:9000/?a1=v1&a2=v2"))
            .build();

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));
    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals(
        "example.ai:9000", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals("/", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals("http", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        "a1=v1&a2=v2", HttpSemanticConventionUtils.getHttpQueryString(rawSpan.getEvent()).get());

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

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));
    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());
    assertEquals(
        "example.ai:9000", HttpSemanticConventionUtils.getHttpHost(rawSpan.getEvent()).get());
    assertEquals(
        "/some-test-path", HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
    assertEquals("http", HttpSemanticConventionUtils.getHttpScheme(rawSpan.getEvent()).get());
    assertEquals(
        "some-query-str=v1",
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

    rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));
    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());
    assertEquals(
        HttpSemanticConventionUtils.getHttpUrl(rawSpan.getEvent()).get(),
        "http://example.internal.com:50850/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel");
  }

  @Test
  public void testSetPath() throws Exception {

    Span span =
        createSpanFromTags(
            Map.of(
                OTelHttpSemanticConventions.HTTP_TARGET.getValue(),
                "/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel"));
    RawSpan rawSpan =
        normalizer.convert(
            "tenant-key",
            span,
            buildEvent(
                "tenant-key", span, new ServiceNamer(ConfigFactory.empty()), Optional.empty()));
    // now, we are not populating first class fields. So, it should be null.
    assertNull(rawSpan.getEvent().getHttp());

    assertEquals(
        "/api/v1/gatekeeper/check",
        HttpSemanticConventionUtils.getHttpPath(rawSpan.getEvent()).get());
  }

  private static Stream<Arguments> provideMapForTestingRequestMethodPriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(HTTP_REQUEST_METHOD), "GET",
            RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD), "PUT");

    Map<String, String> tagsMap2 =
        Map.of(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD), "POST");

    return Stream.of(Arguments.arguments(tagsMap1, "GET"), Arguments.arguments(tagsMap2, "POST"));
  }

  private static Stream<Arguments> provideMapForTestingRequestUrlTagKeysPriority() {

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

    return Stream.of(
        Arguments.arguments(tagsMap1, "https://example.ai/url1"),
        Arguments.arguments(tagsMap2, "https://example.ai/url3"),
        Arguments.arguments(tagsMap3, "https://example.ai/url2"));
  }

  private static Stream<Arguments> provideMapForTestingRequestPathTagKeysPriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(HTTP_REQUEST_PATH), "/path1",
            RawSpanConstants.getValue(HTTP_PATH), "/path2");

    Map<String, String> tagsMap2 = Map.of(RawSpanConstants.getValue(HTTP_PATH), "/path2");

    return Stream.of(
        Arguments.arguments(tagsMap1, "/path1"), Arguments.arguments(tagsMap2, "/path2"));
  }

  private static Stream<Arguments> provideMapForTestingRequestUserAgentTagKeysPriority() {

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

    return Stream.of(
        Arguments.arguments(tagsMap1, "Chrome 1"),
        Arguments.arguments(tagsMap2, "Chrome 2"),
        Arguments.arguments(tagsMap3, "Chrome 3"),
        Arguments.arguments(tagsMap4, "Chrome 4"),
        Arguments.arguments(tagsMap5, "Chrome 5"));
  }

  private static Stream<Arguments> provideMapForTestingRequestSizeTagKeysPriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(ENVOY_REQUEST_SIZE), "50",
            RawSpanConstants.getValue(HTTP_REQUEST_SIZE), "40");

    Map<String, String> tagsMap2 = Map.of(RawSpanConstants.getValue(HTTP_REQUEST_SIZE), "35");

    return Stream.of(Arguments.arguments(tagsMap1, 50), Arguments.arguments(tagsMap2, 35));
  }

  private static Stream<Arguments> provideMapForTestingResponseSizeTagKeysPriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE), "100",
            RawSpanConstants.getValue(HTTP_RESPONSE_SIZE), "90");

    Map<String, String> tagsMap2 = Map.of(RawSpanConstants.getValue(HTTP_RESPONSE_SIZE), "85");

    return Stream.of(Arguments.arguments(tagsMap1, 100), Arguments.arguments(tagsMap2, 85));
  }

  private static Stream<Arguments> provideMapForTestingResponseStatusCodePriority() {

    Map<String, String> tagsMap1 =
        Map.of(
            RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_STATUS_CODE), "200",
            RawSpanConstants.getValue(HTTP_RESPONSE_STATUS_CODE), "201");

    Map<String, String> tagsMap2 =
        Map.of(RawSpanConstants.getValue(HTTP_RESPONSE_STATUS_CODE), "201");

    return Stream.of(Arguments.arguments(tagsMap1, 200), Arguments.arguments(tagsMap2, 201));
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
