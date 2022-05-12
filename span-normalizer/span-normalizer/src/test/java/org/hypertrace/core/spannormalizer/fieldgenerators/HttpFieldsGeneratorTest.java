package org.hypertrace.core.spannormalizer.fieldgenerators;

import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HOST;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HTTP_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HTTP_RESPONSE_BODY;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_AUTHORITY_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_BODY_TRUNCATED;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_CONTENT_LENGTH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_CONTENT_TYPE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_COOKIE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER_COOKIE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HOST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_METHOD;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_PARAM;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_QUERY_STRING;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_X_FORWARDED_FOR_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_BODY_TRUNCATED;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_CONTENT_LENGTH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_CONTENT_TYPE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_COOKIE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_HEADER_SET_COOKIE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_DASH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_UNDERSCORE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_DOT_AGENT;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_METHOD;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_URL;
import static org.hypertrace.core.spannormalizer.utils.TestUtils.createKeyValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Maps;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HttpFieldsGeneratorTest {
  @Test
  public void testHttpFieldsGenerator() {
    String requestBody = "some http request body";
    String responseBody = "{\"a1\": \"v1\", \"b1\": 23}";
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(RawSpanConstants.getValue(HTTP_REQUEST_METHOD), createKeyValue("GET"));
    tagsMap.put(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD), createKeyValue("PUT"));
    tagsMap.put(RawSpanConstants.getValue(HTTP_HTTP_REQUEST_BODY), createKeyValue(requestBody));
    tagsMap.put(RawSpanConstants.getValue(HTTP_HTTP_RESPONSE_BODY), createKeyValue(responseBody));
    tagsMap.put(
        RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL), createKeyValue("https://example.ai/url1"));
    tagsMap.put(RawSpanConstants.getValue(HTTP_URL), createKeyValue("https://example.ai/url2"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_URL), createKeyValue("https://example.ai/url3"));
    tagsMap.put(RawSpanConstants.getValue(HTTP_HOST), createKeyValue("example.ai"));
    tagsMap.put(RawSpanConstants.getValue(HTTP_REQUEST_PATH), createKeyValue("/url1"));
    tagsMap.put(RawSpanConstants.getValue(HTTP_PATH), createKeyValue("/url2"));
    tagsMap.put(RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), createKeyValue("Chrome 1"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE), createKeyValue("Chrome 2"));
    tagsMap.put(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH), createKeyValue("Chrome 3"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), createKeyValue("Chrome 4"));
    tagsMap.put(RawSpanConstants.getValue(HTTP_USER_AGENT), createKeyValue("Chrome 5"));
    tagsMap.put(RawSpanConstants.getValue(HTTP_REQUEST_HOST_HEADER), createKeyValue("example.ai"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_AUTHORITY_HEADER),
        createKeyValue("https:example.ai"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_CONTENT_TYPE), createKeyValue("application/text"));
    tagsMap.put(RawSpanConstants.getValue(HTTP_REQUEST_HEADER_PATH), createKeyValue("/url1_path"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_X_FORWARDED_FOR_HEADER),
        createKeyValue("forwarded for header val"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_HEADER_COOKIE),
        createKeyValue("cookie1=val1; cookie2=val2;"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_CONTENT_TYPE), createKeyValue("application/json"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_HEADER_SET_COOKIE),
        createKeyValue("cookie4=val4;"));
    tagsMap.put(RawSpanConstants.getValue(ENVOY_REQUEST_SIZE), createKeyValue(50));
    tagsMap.put(RawSpanConstants.getValue(HTTP_REQUEST_SIZE), createKeyValue(40));
    tagsMap.put(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE), createKeyValue(30));
    tagsMap.put(RawSpanConstants.getValue(HTTP_RESPONSE_SIZE), createKeyValue(20));
    tagsMap.put(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_STATUS_CODE), createKeyValue(200));
    tagsMap.put(RawSpanConstants.getValue(HTTP_RESPONSE_STATUS_CODE), createKeyValue(201));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_STATUS_MESSAGE), createKeyValue("OK 200 Received"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING), createKeyValue("a1=v1&a2=v2"));

    // Can be put into fields directly
    List<String> directKeysSet = new ArrayList<>(tagsMap.keySet());

    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_HEADER) + ".authorization",
        createKeyValue("Bearer some-auth-header"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_HEADER) + ".x-some-request-header",
        createKeyValue("header-val1"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_HEADER) + ".contentlength", createKeyValue("23"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_HEADER) + ".x-some-response-header",
        createKeyValue("response-header-val2"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_PARAM) + ".param1", createKeyValue("param-val1"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_PARAM) + ".param2", createKeyValue("param-val2"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_COOKIE) + ".cookie1", createKeyValue("cookie-v1"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_COOKIE) + ".cookie2", createKeyValue("cookie-v2"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_COOKIE) + ".responsecookie1",
        createKeyValue("cookie-v11"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_COOKIE) + ".responsecookie2",
        createKeyValue("cookie-v12"));

    // Keys that will be handled by the prefix handler
    List<String> prefixedKeys = new ArrayList<>(tagsMap.keySet());
    prefixedKeys.removeAll(directKeysSet);
    Collections.sort(prefixedKeys);

    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();
    Event.Builder eventBuilder = Event.newBuilder();
    Http.Builder httpBuilder = httpFieldsGenerator.getProtocolBuilder(eventBuilder);

    Assertions.assertSame(eventBuilder.getHttpBuilder(), httpBuilder);

    directKeysSet.forEach(
        key -> httpFieldsGenerator.addValueToBuilder(key, tagsMap.get(key), eventBuilder, tagsMap));
    prefixedKeys.forEach(
        key ->
            httpFieldsGenerator.handleStartsWithKeyIfNecessary(
                key, tagsMap.get(key), eventBuilder));

    assertEquals("GET", httpBuilder.getRequestBuilder().getMethod());
    assertEquals(requestBody, httpBuilder.getRequestBuilder().getBody());
    assertEquals(responseBody, httpBuilder.getResponseBuilder().getBody());
    assertEquals("https://example.ai/url1", httpBuilder.getRequestBuilder().getUrl());
    assertEquals("example.ai", httpBuilder.getRequestBuilder().getHost());
    assertEquals("/url1", httpBuilder.getRequestBuilder().getPath());
    assertEquals("Chrome 1", httpBuilder.getRequestBuilder().getUserAgent());
    assertEquals("example.ai", httpBuilder.getRequestBuilder().getHeadersBuilder().getHost());
    assertEquals(
        "https:example.ai", httpBuilder.getRequestBuilder().getHeadersBuilder().getAuthority());
    assertEquals(
        "application/text", httpBuilder.getRequestBuilder().getHeadersBuilder().getContentType());
    assertEquals("/url1_path", httpBuilder.getRequestBuilder().getHeadersBuilder().getPath());
    assertEquals(
        "forwarded for header val",
        httpBuilder.getRequestBuilder().getHeadersBuilder().getXForwardedFor());
    assertEquals(
        "cookie1=val1; cookie2=val2;",
        httpBuilder.getRequestBuilder().getHeadersBuilder().getCookie());
    assertEquals(
        "application/json", httpBuilder.getResponseBuilder().getHeadersBuilder().getContentType());
    assertEquals(
        "cookie4=val4;", httpBuilder.getResponseBuilder().getHeadersBuilder().getSetCookie());
    assertEquals(50, httpBuilder.getRequestBuilder().getSize());
    assertEquals(30, httpBuilder.getResponseBuilder().getSize());
    assertEquals(200, httpBuilder.getResponseBuilder().getStatusCode());
    assertEquals("OK 200 Received", httpBuilder.getResponseBuilder().getStatusMessage());
    assertEquals("a1=v1&a2=v2", httpBuilder.getRequestBuilder().getQueryString());
    assertEquals(
        Map.of("authorization", "Bearer some-auth-header", "x-some-request-header", "header-val1"),
        httpBuilder.getRequestBuilder().getHeadersBuilder().getOtherHeaders());
    assertEquals(
        Map.of("contentlength", "23", "x-some-response-header", "response-header-val2"),
        httpBuilder.getResponseBuilder().getHeadersBuilder().getOtherHeaders());
    assertEquals(
        Map.of("param1", "param-val1", "param2", "param-val2"),
        httpBuilder.getRequestBuilder().getParams());
    assertEquals(
        List.of("cookie1=cookie-v1", "cookie2=cookie-v2"),
        httpBuilder.getRequestBuilder().getCookies());
    assertEquals(
        List.of("responsecookie1=cookie-v11", "responsecookie2=cookie-v12"),
        httpBuilder.getResponseBuilder().getCookies());
  }

  @Test
  public void testHttpFieldsGeneratorJustPrefixedFields() {
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_HEADER) + ".authorization",
        createKeyValue("Bearer some-auth-header"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_HEADER) + ".x-some-request-header",
        createKeyValue("header-val1"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_HEADER) + ".contentlength", createKeyValue("23"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_HEADER) + ".x-some-response-header",
        createKeyValue("response-header-val2"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_PARAM) + ".param1", createKeyValue("param-val1"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_PARAM) + ".param2", createKeyValue("param-val2"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_COOKIE) + ".cookie1", createKeyValue("cookie-v1"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_COOKIE) + ".cookie2", createKeyValue("cookie-v2"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_COOKIE) + ".responsecookie1",
        createKeyValue("cookie-v11"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_COOKIE) + ".responsecookie2",
        createKeyValue("cookie-v12"));
    // Should not be converted into a fields since they are just prefixes
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_COOKIE) + ".", createKeyValue("cookie-v13"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_HEADER) + ".",
        createKeyValue("Some incomplete header key"));

    List<String> prefixedKeys = new ArrayList<>(tagsMap.keySet());
    Collections.sort(prefixedKeys);

    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();
    Event.Builder eventBuilder = Event.newBuilder();

    prefixedKeys.forEach(
        key ->
            httpFieldsGenerator.handleStartsWithKeyIfNecessary(
                key, tagsMap.get(key), eventBuilder));

    Http.Builder httpBuilder = eventBuilder.getHttpBuilder();
    assertEquals(
        Map.of("authorization", "Bearer some-auth-header", "x-some-request-header", "header-val1"),
        httpBuilder.getRequestBuilder().getHeadersBuilder().getOtherHeaders());
    assertEquals(
        Map.of("contentlength", "23", "x-some-response-header", "response-header-val2"),
        httpBuilder.getResponseBuilder().getHeadersBuilder().getOtherHeaders());
    assertEquals(
        Map.of("param1", "param-val1", "param2", "param-val2"),
        httpBuilder.getRequestBuilder().getParams());
    assertEquals(
        List.of("cookie1=cookie-v1", "cookie2=cookie-v2"),
        httpBuilder.getRequestBuilder().getCookies());
    assertEquals(
        List.of("responsecookie1=cookie-v11", "responsecookie2=cookie-v12"),
        httpBuilder.getResponseBuilder().getCookies());
  }

  @Test
  public void testRequestMethodTagKeysPriority() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap1 = new HashMap<>();
    tagsMap1.put(RawSpanConstants.getValue(HTTP_REQUEST_METHOD), createKeyValue("GET"));
    tagsMap1.put(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD), createKeyValue("PUT"));

    Event.Builder eventBuilder1 = Event.newBuilder();
    tagsMap1.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder1, tagsMap1));

    assertEquals(
        "GET",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder1).getRequestBuilder().getMethod());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD), createKeyValue("POST"));

    Event.Builder eventBuilder2 = Event.newBuilder();
    tagsMap2.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, tagsMap2));

    assertEquals(
        "POST",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder2).getRequestBuilder().getMethod());
  }

  @Test
  public void testRequestUrlTagKeysPriority() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap1 = new HashMap<>();
    tagsMap1.put(
        RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL), createKeyValue("https://example.ai/url1"));
    tagsMap1.put(RawSpanConstants.getValue(HTTP_URL), createKeyValue("https://example.ai/url2"));
    tagsMap1.put(
        RawSpanConstants.getValue(HTTP_REQUEST_URL), createKeyValue("https://example.ai/url3"));

    Event.Builder eventBuilder1 = Event.newBuilder();
    tagsMap1.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder1, tagsMap1));

    assertEquals(
        "https://example.ai/url1",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder1).getRequestBuilder().getUrl());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(RawSpanConstants.getValue(HTTP_URL), createKeyValue("https://example.ai/url2"));
    tagsMap2.put(
        RawSpanConstants.getValue(HTTP_REQUEST_URL), createKeyValue("https://example.ai/url3"));

    Event.Builder eventBuilder2 = Event.newBuilder();
    tagsMap2.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, tagsMap2));

    assertEquals(
        "https://example.ai/url3",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder2).getRequestBuilder().getUrl());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap3 = new HashMap<>();
    tagsMap3.put(RawSpanConstants.getValue(HTTP_URL), createKeyValue("https://example.ai/url2"));

    Event.Builder eventBuilder3 = Event.newBuilder();
    tagsMap3.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder3, tagsMap3));

    assertEquals(
        "https://example.ai/url2",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder3).getRequestBuilder().getUrl());
  }

  @Test
  public void testRequestPathTagKeysPriority() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap1 = new HashMap<>();
    tagsMap1.put(RawSpanConstants.getValue(HTTP_REQUEST_PATH), createKeyValue("/path1"));
    tagsMap1.put(RawSpanConstants.getValue(HTTP_PATH), createKeyValue("/path2"));

    Event.Builder eventBuilder1 = Event.newBuilder();
    tagsMap1.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder1, tagsMap1));

    assertEquals(
        "/path1",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder1).getRequestBuilder().getPath());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(RawSpanConstants.getValue(HTTP_PATH), createKeyValue("/path2"));

    Event.Builder eventBuilder2 = Event.newBuilder();
    tagsMap2.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, tagsMap2));

    assertEquals(
        "/path2",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder2).getRequestBuilder().getPath());
  }

  @Test
  public void testInvalidRequestPath() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap1 = new HashMap<>();
    tagsMap1.put(RawSpanConstants.getValue(HTTP_REQUEST_PATH), createKeyValue("path1"));
    tagsMap1.put(RawSpanConstants.getValue(HTTP_PATH), createKeyValue("  "));

    Event.Builder eventBuilder1 = Event.newBuilder();
    tagsMap1.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder1, tagsMap1));

    Assertions.assertFalse(
        httpFieldsGenerator.getProtocolBuilder(eventBuilder1).getRequestBuilder().hasPath());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(RawSpanConstants.getValue(HTTP_REQUEST_PATH), createKeyValue("path1"));
    tagsMap2.put(RawSpanConstants.getValue(HTTP_PATH), createKeyValue("/"));

    Event.Builder eventBuilder2 = Event.newBuilder();
    tagsMap2.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, tagsMap2));

    assertEquals(
        "/", httpFieldsGenerator.getProtocolBuilder(eventBuilder2).getRequestBuilder().getPath());
  }

  @Test
  public void testRequestUserAgentTagKeysPriority() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap1 = new HashMap<>();
    tagsMap1.put(RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), createKeyValue("Chrome 1"));
    tagsMap1.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE), createKeyValue("Chrome 2"));
    tagsMap1.put(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH), createKeyValue("Chrome 3"));
    tagsMap1.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), createKeyValue("Chrome 4"));
    tagsMap1.put(RawSpanConstants.getValue(HTTP_USER_AGENT), createKeyValue("Chrome 5"));

    Event.Builder eventBuilder1 = Event.newBuilder();
    tagsMap1.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder1, tagsMap1));

    assertEquals(
        "Chrome 1",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder1).getRequestBuilder().getUserAgent());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE), createKeyValue("Chrome 2"));
    tagsMap2.put(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH), createKeyValue("Chrome 3"));
    tagsMap2.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), createKeyValue("Chrome 4"));
    tagsMap2.put(RawSpanConstants.getValue(HTTP_USER_AGENT), createKeyValue("Chrome 5"));

    Event.Builder eventBuilder2 = Event.newBuilder();
    tagsMap2.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, tagsMap2));

    assertEquals(
        "Chrome 2",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder2).getRequestBuilder().getUserAgent());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap3 = new HashMap<>();
    tagsMap3.put(RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH), createKeyValue("Chrome 3"));
    tagsMap3.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), createKeyValue("Chrome 4"));
    tagsMap3.put(RawSpanConstants.getValue(HTTP_USER_AGENT), createKeyValue("Chrome 5"));

    Event.Builder eventBuilder3 = Event.newBuilder();
    tagsMap3.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder3, tagsMap3));

    assertEquals(
        "Chrome 3",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder3).getRequestBuilder().getUserAgent());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap4 = new HashMap<>();
    tagsMap4.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), createKeyValue("Chrome 4"));
    tagsMap4.put(RawSpanConstants.getValue(HTTP_USER_AGENT), createKeyValue("Chrome 5"));

    Event.Builder eventBuilder4 = Event.newBuilder();
    tagsMap4.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder4, tagsMap4));

    assertEquals(
        "Chrome 4",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder4).getRequestBuilder().getUserAgent());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap5 = new HashMap<>();
    tagsMap5.put(RawSpanConstants.getValue(HTTP_USER_AGENT), createKeyValue("Chrome 5"));

    Event.Builder eventBuilder5 = Event.newBuilder();
    tagsMap5.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder5, tagsMap5));

    assertEquals(
        "Chrome 5",
        httpFieldsGenerator.getProtocolBuilder(eventBuilder5).getRequestBuilder().getUserAgent());
  }

  @Test
  public void testRequestSizeTagKeysPriority() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap1 = new HashMap<>();
    tagsMap1.put(RawSpanConstants.getValue(ENVOY_REQUEST_SIZE), createKeyValue(50));
    tagsMap1.put(RawSpanConstants.getValue(HTTP_REQUEST_SIZE), createKeyValue(40));
    tagsMap1.put(OTelHttpSemanticConventions.HTTP_REQUEST_SIZE.getValue(), createKeyValue(30));
    tagsMap1.put(RawSpanConstants.getValue(HTTP_REQUEST_CONTENT_LENGTH), createKeyValue(20));
    tagsMap1.put(
        RawSpanConstants.getValue(HTTP_HTTP_REQUEST_BODY), createKeyValue("Hello, there!"));

    Event.Builder eventBuilder1 = Event.newBuilder();
    Http.Builder httpBuilder1 = httpFieldsGenerator.getProtocolBuilder(eventBuilder1);

    tagsMap1.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder1, tagsMap1));

    assertEquals(50, httpBuilder1.getRequestBuilder().getSize());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(RawSpanConstants.getValue(HTTP_REQUEST_SIZE), createKeyValue(35));
    tagsMap2.put(OTelHttpSemanticConventions.HTTP_REQUEST_SIZE.getValue(), createKeyValue(30));
    tagsMap2.put(RawSpanConstants.getValue(HTTP_REQUEST_CONTENT_LENGTH), createKeyValue(20));

    Event.Builder eventBuilder2 = Event.newBuilder();
    Http.Builder httpBuilder2 = httpFieldsGenerator.getProtocolBuilder(eventBuilder2);

    tagsMap2.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, tagsMap2));

    assertEquals(35, httpBuilder2.getRequestBuilder().getSize());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap3 = new HashMap<>();
    tagsMap3.put(OTelHttpSemanticConventions.HTTP_REQUEST_SIZE.getValue(), createKeyValue(30));
    tagsMap3.put(RawSpanConstants.getValue(HTTP_REQUEST_CONTENT_LENGTH), createKeyValue(20));

    Event.Builder eventBuilder3 = Event.newBuilder();
    Http.Builder httpBuilder3 = httpFieldsGenerator.getProtocolBuilder(eventBuilder3);

    tagsMap3.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder3, tagsMap3));

    assertEquals(30, httpBuilder3.getRequestBuilder().getSize());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap4 = new HashMap<>();
    tagsMap4.put(RawSpanConstants.getValue(HTTP_REQUEST_CONTENT_LENGTH), createKeyValue(20));

    Event.Builder eventBuilder4 = Event.newBuilder();
    Http.Builder httpBuilder4 = httpFieldsGenerator.getProtocolBuilder(eventBuilder4);

    tagsMap4.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder4, tagsMap4));

    assertEquals(20, httpBuilder4.getRequestBuilder().getSize());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap5 = new HashMap<>();
    tagsMap5.put(
        RawSpanConstants.getValue(HTTP_HTTP_REQUEST_BODY), createKeyValue("Hello, there!"));

    Event.Builder eventBuilder5 = Event.newBuilder();
    Http.Builder httpBuilder5 = httpFieldsGenerator.getProtocolBuilder(eventBuilder5);

    tagsMap5.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder5, tagsMap5));

    assertEquals(13, httpBuilder5.getRequestBuilder().getSize());

    // test for truncated body
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap6 = new HashMap<>();
    tagsMap6.put(
        RawSpanConstants.getValue(HTTP_HTTP_REQUEST_BODY), createKeyValue("Hello, there!"));
    Map<String, JaegerSpanInternalModel.KeyValue> allTags = new HashMap<>();
    allTags.putAll(tagsMap6);
    allTags.put(RawSpanConstants.getValue(HTTP_REQUEST_BODY_TRUNCATED), createKeyValue("true"));

    Event.Builder eventBuilder6 = Event.newBuilder();
    Http.Builder httpBuilder6 = httpFieldsGenerator.getProtocolBuilder(eventBuilder6);

    tagsMap6.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder6, allTags));

    assertEquals(0, httpBuilder6.getRequestBuilder().getSize());
  }

  @Test
  public void testResponseSizeTagKeysPriority() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap1 = new HashMap<>();
    tagsMap1.put(RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE), createKeyValue(100));
    tagsMap1.put(RawSpanConstants.getValue(HTTP_RESPONSE_SIZE), createKeyValue(90));
    tagsMap1.put(OTelHttpSemanticConventions.HTTP_RESPONSE_SIZE.getValue(), createKeyValue(80));
    tagsMap1.put(RawSpanConstants.getValue(HTTP_RESPONSE_CONTENT_LENGTH), createKeyValue(60));
    tagsMap1.put(
        RawSpanConstants.getValue(HTTP_HTTP_RESPONSE_BODY), createKeyValue("Hello World!"));

    Event.Builder eventBuilder1 = Event.newBuilder();
    Http.Builder httpBuilder1 = httpFieldsGenerator.getProtocolBuilder(eventBuilder1);

    tagsMap1.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder1, tagsMap1));

    assertEquals(100, httpBuilder1.getResponseBuilder().getSize());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(RawSpanConstants.getValue(HTTP_RESPONSE_SIZE), createKeyValue(85));
    tagsMap2.put(OTelHttpSemanticConventions.HTTP_RESPONSE_SIZE.getValue(), createKeyValue(80));
    tagsMap2.put(RawSpanConstants.getValue(HTTP_RESPONSE_CONTENT_LENGTH), createKeyValue(60));

    Event.Builder eventBuilder2 = Event.newBuilder();
    Http.Builder httpBuilder2 = httpFieldsGenerator.getProtocolBuilder(eventBuilder2);

    tagsMap2.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, tagsMap2));

    assertEquals(85, httpBuilder2.getResponseBuilder().getSize());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap3 = new HashMap<>();
    tagsMap3.put(OTelHttpSemanticConventions.HTTP_RESPONSE_SIZE.getValue(), createKeyValue(80));
    tagsMap3.put(RawSpanConstants.getValue(HTTP_RESPONSE_CONTENT_LENGTH), createKeyValue(60));

    Event.Builder eventBuilder3 = Event.newBuilder();
    Http.Builder httpBuilder3 = httpFieldsGenerator.getProtocolBuilder(eventBuilder3);

    tagsMap3.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder3, tagsMap3));

    assertEquals(80, httpBuilder3.getResponseBuilder().getSize());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap4 = new HashMap<>();
    tagsMap4.put(RawSpanConstants.getValue(HTTP_RESPONSE_CONTENT_LENGTH), createKeyValue(60));

    Event.Builder eventBuilder4 = Event.newBuilder();
    Http.Builder httpBuilder4 = httpFieldsGenerator.getProtocolBuilder(eventBuilder4);

    tagsMap4.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder4, tagsMap4));

    assertEquals(60, httpBuilder4.getResponseBuilder().getSize());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap5 = new HashMap<>();
    tagsMap5.put(
        RawSpanConstants.getValue(HTTP_HTTP_RESPONSE_BODY), createKeyValue("Hello World!"));

    Event.Builder eventBuilder5 = Event.newBuilder();
    Http.Builder httpBuilder5 = httpFieldsGenerator.getProtocolBuilder(eventBuilder5);

    tagsMap5.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder5, tagsMap5));

    assertEquals(12, httpBuilder5.getResponseBuilder().getSize());

    // test for truncated reponse body
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap6 = new HashMap<>();
    tagsMap6.put(
        RawSpanConstants.getValue(HTTP_HTTP_RESPONSE_BODY), createKeyValue("Hello World!"));
    Map<String, JaegerSpanInternalModel.KeyValue> allTags = new HashMap<>();
    allTags.putAll(tagsMap6);
    allTags.put(RawSpanConstants.getValue(HTTP_RESPONSE_BODY_TRUNCATED), createKeyValue("true"));

    Event.Builder eventBuilder6 = Event.newBuilder();
    Http.Builder httpBuilder6 = httpFieldsGenerator.getProtocolBuilder(eventBuilder6);

    tagsMap5.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder6, allTags));

    assertEquals(0, httpBuilder6.getResponseBuilder().getSize());
  }

  @Test
  public void testResponseStatusCodeTagKeysPriority() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap1 = new HashMap<>();
    tagsMap1.put(RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_STATUS_CODE), createKeyValue(200));
    tagsMap1.put(RawSpanConstants.getValue(HTTP_RESPONSE_STATUS_CODE), createKeyValue(201));

    Event.Builder eventBuilder1 = Event.newBuilder();
    Http.Builder httpBuilder1 = httpFieldsGenerator.getProtocolBuilder(eventBuilder1);

    tagsMap1.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder1, tagsMap1));

    assertEquals(200, httpBuilder1.getResponseBuilder().getStatusCode());

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap2 = new HashMap<>();
    tagsMap2.put(RawSpanConstants.getValue(HTTP_RESPONSE_STATUS_CODE), createKeyValue(501));

    Event.Builder eventBuilder2 = Event.newBuilder();
    Http.Builder httpBuilder2 = httpFieldsGenerator.getProtocolBuilder(eventBuilder2);

    tagsMap2.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder2, tagsMap2));

    assertEquals(501, httpBuilder2.getResponseBuilder().getStatusCode());
  }

  @Test
  public void testRelativeUrlNotSetsUrlField() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();

    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(RawSpanConstants.getValue(HTTP_URL), createKeyValue("/dispatch/test?a=b&k1=v1"));

    Event.Builder eventBuilder = Event.newBuilder();
    Http.Builder httpBuilder = httpFieldsGenerator.getProtocolBuilder(eventBuilder);

    tagsMap.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap));
    assertEquals("/dispatch/test?a=b&k1=v1", httpBuilder.getRequestBuilder().getUrl());

    httpFieldsGenerator.populateOtherFields(
        eventBuilder, Maps.newHashMap()); // this should unset the url field
    Assertions.assertNull(httpBuilder.getRequestBuilder().getUrl());
  }

  @Test
  public void testAbsoluteUrlSetsUrlField() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_URL),
        createKeyValue("http://abc.xyz/dispatch/test?a=b&k1=v1"));

    Event.Builder eventBuilder = Event.newBuilder();
    Http.Builder httpBuilder = httpFieldsGenerator.getProtocolBuilder(eventBuilder);

    tagsMap.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap));
    httpFieldsGenerator.populateOtherFields(eventBuilder, Maps.newHashMap());

    assertEquals(
        "http://abc.xyz/dispatch/test?a=b&k1=v1", httpBuilder.getRequestBuilder().getUrl());
  }

  @Test
  public void testInvalidUrlRejectedByUrlValidator() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_URL),
        createKeyValue("xyz://abc.xyz/dispatch/test?a=b&k1=v1"));

    Event.Builder eventBuilder = Event.newBuilder();
    Http.Builder httpBuilder = httpFieldsGenerator.getProtocolBuilder(eventBuilder);

    tagsMap.forEach(
        (key, keyValue) ->
            httpFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap));
    Assertions.assertNull(httpBuilder.getRequestBuilder().getUrl());
  }

  @Test
  public void testPopulateOtherFields() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();

    Event.Builder eventBuilder = Event.newBuilder();

    httpFieldsGenerator.populateOtherFields(eventBuilder, Maps.newHashMap());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getUrl());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    eventBuilder
        .getHttpBuilder()
        .getRequestBuilder()
        .setUrl("https://example.ai/apis/5673/events?a1=v1&a2=v2");
    httpFieldsGenerator.populateOtherFields(eventBuilder, Maps.newHashMap());

    assertEquals("https", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/apis/5673/events", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    assertEquals("a1=v1&a2=v2", eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    eventBuilder = Event.newBuilder();

    // Removes the trailing "/" for path
    eventBuilder
        .getHttpBuilder()
        .getRequestBuilder()
        .setUrl("https://example.ai/apis/5673/events/?a1=v1&a2=v2");
    httpFieldsGenerator.populateOtherFields(eventBuilder, Maps.newHashMap());

    assertEquals("https", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/apis/5673/events", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    assertEquals("a1=v1&a2=v2", eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    // No query
    eventBuilder = Event.newBuilder();
    eventBuilder.getHttpBuilder().getRequestBuilder().setUrl("https://example.ai/apis/5673/events");
    httpFieldsGenerator.populateOtherFields(eventBuilder, Maps.newHashMap());

    assertEquals("https", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/apis/5673/events", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    // No path
    eventBuilder = Event.newBuilder();
    eventBuilder.getHttpBuilder().getRequestBuilder().setUrl("https://example.ai");
    httpFieldsGenerator.populateOtherFields(eventBuilder, Maps.newHashMap());

    assertEquals("https", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    // Relative URL - should extract path and query string only
    eventBuilder = Event.newBuilder();
    eventBuilder.getHttpBuilder().getRequestBuilder().setUrl("/apis/5673/events?a1=v1&a2=v2");
    httpFieldsGenerator.populateOtherFields(eventBuilder, Maps.newHashMap());

    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getUrl());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/apis/5673/events", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    assertEquals("a1=v1&a2=v2", eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    // "/" home path, host with port
    eventBuilder = Event.newBuilder();
    eventBuilder.getHttpBuilder().getRequestBuilder().setUrl("http://example.ai:9000/?a1=v1&a2=v2");
    httpFieldsGenerator.populateOtherFields(eventBuilder, Maps.newHashMap());

    assertEquals("http", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai:9000", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    assertEquals("a1=v1&a2=v2", eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    // Set path and query string before calling populateOtherFields. Simulate case where fields came
    // from attributes
    eventBuilder = Event.newBuilder();
    eventBuilder
        .getHttpBuilder()
        .getRequestBuilder()
        .setUrl("http://example.ai:9000/apis/4533?a1=v1&a2=v2");
    eventBuilder.getHttpBuilder().getRequestBuilder().setQueryString("some-query-str=v1");
    eventBuilder.getHttpBuilder().getRequestBuilder().setPath("/some-test-path");
    httpFieldsGenerator.populateOtherFields(eventBuilder, Maps.newHashMap());

    assertEquals("http", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai:9000", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/some-test-path", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    assertEquals(
        "some-query-str=v1", eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());
  }

  @Test
  public void testHttpFieldGenerationForOtelSpan() {
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(OTelHttpSemanticConventions.HTTP_TARGET.getValue(), createKeyValue("/url2"));
    tagsMap.put(
        OTelHttpSemanticConventions.HTTP_URL.getValue(), createKeyValue("https://example.ai/url1"));
    tagsMap.put(OTelHttpSemanticConventions.HTTP_USER_AGENT.getValue(), createKeyValue("Chrome 1"));
    tagsMap.put(OTelHttpSemanticConventions.HTTP_REQUEST_SIZE.getValue(), createKeyValue(100));
    tagsMap.put(OTelHttpSemanticConventions.HTTP_RESPONSE_SIZE.getValue(), createKeyValue(200));
    tagsMap.put(OTelHttpSemanticConventions.HTTP_STATUS_CODE.getValue(), createKeyValue(400));
    tagsMap.put(OTelHttpSemanticConventions.HTTP_METHOD.getValue(), createKeyValue("GET"));
    tagsMap.put(OTelHttpSemanticConventions.HTTP_SCHEME.getValue(), createKeyValue("https"));
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();
    Event.Builder eventBuilder = Event.newBuilder();
    Http.Builder httpBuilder = httpFieldsGenerator.getProtocolBuilder(eventBuilder);
    List<String> directKeysSet = new ArrayList<>(tagsMap.keySet());
    directKeysSet.forEach(
        key -> httpFieldsGenerator.addValueToBuilder(key, tagsMap.get(key), eventBuilder, tagsMap));

    assertEquals("GET", httpBuilder.getRequestBuilder().getMethod());
    assertEquals("https://example.ai/url1", httpBuilder.getRequestBuilder().getUrl());
    assertEquals("/url2", httpBuilder.getRequestBuilder().getPath());
    assertEquals("Chrome 1", httpBuilder.getRequestBuilder().getUserAgent());
    assertEquals(100, httpBuilder.getRequestBuilder().getSize());
    assertEquals(200, httpBuilder.getResponseBuilder().getSize());
    assertEquals(400, httpBuilder.getResponseBuilder().getStatusCode());
    assertEquals("https", httpBuilder.getRequestBuilder().getScheme());
  }

  @Test
  public void testPopulateOtherFieldsOTelSpan() {
    HttpFieldsGenerator httpFieldsGenerator = new HttpFieldsGenerator();

    Event.Builder eventBuilder = Event.newBuilder();

    httpFieldsGenerator.populateOtherFields(eventBuilder, Maps.newHashMap());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getUrl());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    String url = "https://example.ai/apis/5673/events?a1=v1&a2=v2";
    Map<String, AttributeValue> map = Maps.newHashMap();
    map.put(OTelHttpSemanticConventions.HTTP_URL.getValue(), buildAttributeValue(url));
    httpFieldsGenerator.populateOtherFields(eventBuilder, map);

    assertEquals("https", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/apis/5673/events", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    assertEquals("a1=v1&a2=v2", eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    eventBuilder = Event.newBuilder();

    // Removes the trailing "/" for path
    url = "https://example.ai/apis/5673/events/?a1=v1&a2=v2";
    map = Maps.newHashMap();
    map.put(OTelHttpSemanticConventions.HTTP_URL.getValue(), buildAttributeValue(url));
    httpFieldsGenerator.populateOtherFields(eventBuilder, map);

    assertEquals("https", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/apis/5673/events", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    assertEquals("a1=v1&a2=v2", eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    // No query
    eventBuilder = Event.newBuilder();
    url = "https://example.ai/apis/5673/events";
    map = Maps.newHashMap();
    map.put(OTelHttpSemanticConventions.HTTP_URL.getValue(), buildAttributeValue(url));
    httpFieldsGenerator.populateOtherFields(eventBuilder, map);

    assertEquals("https", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/apis/5673/events", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    // No path
    eventBuilder = Event.newBuilder();
    url = "https://example.ai";
    map = Maps.newHashMap();
    map.put(OTelHttpSemanticConventions.HTTP_URL.getValue(), buildAttributeValue(url));
    httpFieldsGenerator.populateOtherFields(eventBuilder, map);

    assertEquals("https", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    // Relative URL - should extract path and query string only
    eventBuilder = Event.newBuilder();
    eventBuilder.getHttpBuilder().getRequestBuilder().setUrl("/apis/5673/events?a1=v1&a2=v2");
    map = Maps.newHashMap();
    httpFieldsGenerator.populateOtherFields(eventBuilder, map);

    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getUrl());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    Assertions.assertNull(eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/apis/5673/events", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    assertEquals("a1=v1&a2=v2", eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    // "/" home path, host with port
    eventBuilder = Event.newBuilder();
    url = "http://example.ai:9000/?a1=v1&a2=v2";
    map = Maps.newHashMap();
    map.put(OTelHttpSemanticConventions.HTTP_URL.getValue(), buildAttributeValue(url));
    httpFieldsGenerator.populateOtherFields(eventBuilder, map);
    assertEquals("http", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai:9000", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    assertEquals("a1=v1&a2=v2", eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    // Set path and query string before calling populateOtherFields. Simulate case where fields came
    // from attributes
    eventBuilder = Event.newBuilder();
    url = "http://example.ai:9000/apis/4533?a1=v1&a2=v2";
    map = Maps.newHashMap();
    map.put(OTelHttpSemanticConventions.HTTP_URL.getValue(), buildAttributeValue(url));
    httpFieldsGenerator.populateOtherFields(eventBuilder, map);
    eventBuilder.getHttpBuilder().getRequestBuilder().setQueryString("some-query-str=v1");
    eventBuilder.getHttpBuilder().getRequestBuilder().setPath("/some-test-path");
    httpFieldsGenerator.populateOtherFields(eventBuilder, Maps.newHashMap());

    assertEquals("http", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    assertEquals("example.ai:9000", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    assertEquals("/some-test-path", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
    assertEquals(
        "some-query-str=v1", eventBuilder.getHttpBuilder().getRequestBuilder().getQueryString());

    // originally set url is a relative url, should be overridden
    eventBuilder = Event.newBuilder();
    eventBuilder
        .getHttpBuilder()
        .getRequestBuilder()
        .setUrl(
            "/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel");
    map = Maps.newHashMap();
    map.put(OTelHttpSemanticConventions.HTTP_SCHEME.getValue(), buildAttributeValue("http"));
    map.put(OTelSpanSemanticConventions.SPAN_KIND.getValue(), buildAttributeValue("server"));
    map.put(
        OTelHttpSemanticConventions.HTTP_NET_HOST_NAME.getValue(),
        buildAttributeValue("example.internal.com"));
    map.put(
        OTelHttpSemanticConventions.HTTP_NET_HOST_PORT.getValue(), buildAttributeValue("50850"));
    map.put(
        OTelHttpSemanticConventions.HTTP_TARGET.getValue(),
        buildAttributeValue(
            "/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel"));
    httpFieldsGenerator.populateOtherFields(eventBuilder, map);
    assertEquals(
        "http://example.internal.com:50850/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel",
        eventBuilder.getHttpBuilder().getRequestBuilder().getUrl());
  }

  @Test
  public void testIsAbsoluteUrl() {
    assertTrue(HttpFieldsGenerator.isAbsoluteUrl("http://example.com/abc/xyz"));
    assertFalse(HttpFieldsGenerator.isAbsoluteUrl("/abc/xyz"));
  }

  @Test
  public void testGetPathFromUrl() {
    Optional<String> path =
        HttpFieldsGenerator.getPathFromUrlObject(
            "/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel");
    assertEquals(path.get(), "/api/v1/gatekeeper/check");
  }

  @Test
  public void testSetPath() {
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(
        OTelHttpSemanticConventions.HTTP_TARGET.getValue(),
        createKeyValue(
            "/api/v1/gatekeeper/check?url=%2Fpixel%2Factivities%3Fadvertisable%3DTRHRT&method=GET&service=pixel"));
    Http.Builder builder = Http.newBuilder().setRequestBuilder(Request.newBuilder());
    HttpFieldsGenerator.setPath(builder, tagsMap);
    assertEquals(builder.getRequestBuilder().getPath(), "/api/v1/gatekeeper/check");
  }

  private static AttributeValue buildAttributeValue(String value) {
    return AttributeValue.newBuilder().setValue(value).build();
  }
}
