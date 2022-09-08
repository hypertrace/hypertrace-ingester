package org.hypertrace.semantic.convention.utils.http;

import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_HOST;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_NET_HOST_NAME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_NET_HOST_PORT;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_SCHEME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_SERVER_NAME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_TARGET;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_URL;
import static org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions.SPAN_KIND;
import static org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions.SPAN_KIND_CLIENT_VALUE;
import static org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions.SPAN_KIND_SERVER_VALUE;
import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_CENSUS_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HTTP_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HTTP_RESPONSE_BODY;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_BODY_TRUNCATED;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_CONTENT_LENGTH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_CONTENT_TYPE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_METHOD;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_QUERY_STRING;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_X_FORWARDED_FOR_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_BODY_TRUNCATED;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_CONTENT_LENGTH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_DASH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_UNDERSCORE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_DOT_AGENT;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_METHOD;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_URL;
import static org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil.buildAttributeValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.semantic.convention.constants.http.HttpSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.core.span.constants.v1.OCAttribute;
import org.hypertrace.core.span.constants.v1.OCSpanKind;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit test for {@link HttpSemanticConventionUtils} */
public class HttpSemanticConventionUtilsTest {
  private Event createMockEventWithAttribute(String key, String value) {
    Event e = mock(Event.class);
    when(e.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(Map.of(key, AttributeValue.newBuilder().setValue(value).build()))
                .build());
    when(e.getEnrichedAttributes()).thenReturn(null);
    return e;
  }

  @Test
  public void testGetHttpUrlForOtelFormat() {
    // http url present
    Map<String, AttributeValue> map = Maps.newHashMap();
    map.put(
        HTTP_URL.getValue(), buildAttributeValue("https://172.0.8.11:1211/webshop/articles/4?s=1"));
    String url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://172.0.8.11:1211/webshop/articles/4?s=1", url);

    // host & target present
    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(
        HttpSemanticConventions.HTTP_REQUEST_X_FORWARDED_PROTO.getValue(),
        buildAttributeValue("http"));
    map.put(HTTP_HOST.getValue(), buildAttributeValue("example.com:1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("http://example.com:1211/webshop/articles/4?s=1", url);

    // client span, span_kind present
    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(
        OTelSpanSemanticConventions.NET_PEER_NAME.getValue(), buildAttributeValue("example.com"));
    map.put(OTelSpanSemanticConventions.NET_PEER_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(SPAN_KIND.getValue(), buildAttributeValue(SPAN_KIND_CLIENT_VALUE.getValue()));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://example.com:1211/webshop/articles/4?s=1", url);

    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(
        HttpSemanticConventions.HTTP_REQUEST_FORWARDED.getValue(),
        buildAttributeValue("by=random;proto=http"));
    map.put(OTelSpanSemanticConventions.NET_PEER_IP.getValue(), buildAttributeValue("172.0.8.11"));
    map.put(OTelSpanSemanticConventions.NET_PEER_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(SPAN_KIND.getValue(), buildAttributeValue(SPAN_KIND_CLIENT_VALUE.getValue()));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("http://172.0.8.11:1211/webshop/articles/4?s=1", url);

    // client span, span.kind present
    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(
        OTelSpanSemanticConventions.NET_PEER_NAME.getValue(), buildAttributeValue("example.com"));
    map.put(OTelSpanSemanticConventions.NET_PEER_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(
        RawSpanConstants.getValue(OCAttribute.OC_ATTRIBUTE_SPAN_KIND),
        buildAttributeValue(RawSpanConstants.getValue(OCSpanKind.OC_SPAN_KIND_CLIENT)));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://example.com:1211/webshop/articles/4?s=1", url);

    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(OTelSpanSemanticConventions.NET_PEER_IP.getValue(), buildAttributeValue("172.0.8.11"));
    map.put(OTelSpanSemanticConventions.NET_PEER_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(
        RawSpanConstants.getValue(OCAttribute.OC_ATTRIBUTE_SPAN_KIND),
        buildAttributeValue(RawSpanConstants.getValue(OCSpanKind.OC_SPAN_KIND_CLIENT)));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://172.0.8.11:1211/webshop/articles/4?s=1", url);

    // server span, span_kind present
    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(HTTP_SERVER_NAME.getValue(), buildAttributeValue("example.com"));
    map.put(HTTP_NET_HOST_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(SPAN_KIND.getValue(), buildAttributeValue(SPAN_KIND_SERVER_VALUE.getValue()));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://example.com:1211/webshop/articles/4?s=1", url);

    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(HTTP_NET_HOST_NAME.getValue(), buildAttributeValue("example.com"));
    map.put(HTTP_NET_HOST_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(SPAN_KIND.getValue(), buildAttributeValue(SPAN_KIND_SERVER_VALUE.getValue()));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://example.com:1211/webshop/articles/4?s=1", url);

    // server span, span.kind present
    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(HTTP_SERVER_NAME.getValue(), buildAttributeValue("example.com"));
    map.put(HTTP_NET_HOST_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(
        RawSpanConstants.getValue(OCAttribute.OC_ATTRIBUTE_SPAN_KIND),
        buildAttributeValue(RawSpanConstants.getValue(OCSpanKind.OC_SPAN_KIND_SERVER)));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://example.com:1211/webshop/articles/4?s=1", url);

    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(HTTP_NET_HOST_NAME.getValue(), buildAttributeValue("example.com"));
    map.put(HTTP_NET_HOST_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(
        RawSpanConstants.getValue(OCAttribute.OC_ATTRIBUTE_SPAN_KIND),
        buildAttributeValue(RawSpanConstants.getValue(OCSpanKind.OC_SPAN_KIND_SERVER)));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://example.com:1211/webshop/articles/4?s=1", url);
  }

  @Test
  public void testGetHttpUserAgent() {
    Event event =
        createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), "Chrome 1");
    assertEquals(Optional.of("Chrome 1"), HttpSemanticConventionUtils.getHttpUserAgent(event));

    event = mock(Event.class);
    assertTrue(HttpSemanticConventionUtils.getHttpUserAgent(event).isEmpty());

    event = createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), "");
    assertTrue(HttpSemanticConventionUtils.getHttpUserAgent(event).isEmpty());

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RawSpanConstants.getValue(HTTP_USER_DOT_AGENT),
                        AttributeValue.newBuilder().setValue("Chrome 1").build(),
                        RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE),
                        AttributeValue.newBuilder().setValue("Chrome 2").build(),
                        RawSpanConstants.getValue(CENSUS_RESPONSE_CENSUS_STATUS_CODE),
                        AttributeValue.newBuilder().setValue("Chrome 3").build(),
                        RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH),
                        AttributeValue.newBuilder().setValue("Chrome 4").build()))
                .build());
    assertEquals(Optional.of("Chrome 1"), HttpSemanticConventionUtils.getHttpUserAgent(event));
  }

  @Test
  public void testGetHttpUserAgentFromHeader() {
    Event event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), "Chrome 1");
    assertEquals(
        Optional.of("Chrome 1"), HttpSemanticConventionUtils.getHttpUserAgentFromHeader(event));

    event = mock(Event.class);
    assertTrue(HttpSemanticConventionUtils.getHttpUserAgentFromHeader(event).isEmpty());

    event =
        createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), "Chrome 1");
    assertTrue(HttpSemanticConventionUtils.getHttpUserAgentFromHeader(event).isEmpty());
  }

  @Test
  public void testGetHttpHost() {
    Event event = createMockEventWithAttribute(RawSpanConstants.getValue(Http.HTTP_HOST), "abc.ai");
    assertEquals(Optional.of("abc.ai"), HttpSemanticConventionUtils.getHttpHost(event));

    event = mock(Event.class);
    assertTrue(HttpSemanticConventionUtils.getHttpHost(event).isEmpty());
  }

  @Test
  public void testGetHttpPath() {
    Event event =
        createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_REQUEST_PATH), "/path");
    assertEquals(Optional.of("/path"), HttpSemanticConventionUtils.getHttpPath(event));

    event = mock(Event.class);
    assertTrue(HttpSemanticConventionUtils.getHttpPath(event).isEmpty());

    event = createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_REQUEST_PATH), "");
    assertTrue(HttpSemanticConventionUtils.getHttpPath(event).isEmpty());

    event = createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_REQUEST_PATH), "path");
    assertTrue(HttpSemanticConventionUtils.getHttpPath(event).isEmpty());

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RawSpanConstants.getValue(HTTP_REQUEST_PATH),
                        AttributeValue.newBuilder().setValue("/path1").build(),
                        RawSpanConstants.getValue(HTTP_PATH),
                        AttributeValue.newBuilder().setValue("/path2").build(),
                        OTelHttpSemanticConventions.HTTP_TARGET.getValue(),
                        AttributeValue.newBuilder().setValue("/path3").build()))
                .build());
    assertEquals(Optional.of("/path1"), HttpSemanticConventionUtils.getHttpPath(event));
  }

  @Test
  public void testGetHttpMethod() {
    Event event =
        createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_REQUEST_METHOD), "GET");
    assertEquals(Optional.of("GET"), HttpSemanticConventionUtils.getHttpMethod(event));

    event = mock(Event.class);
    assertTrue(HttpSemanticConventionUtils.getHttpMethod(event).isEmpty());

    event = createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_REQUEST_METHOD), "");
    assertTrue(HttpSemanticConventionUtils.getHttpPath(event).isEmpty());

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RawSpanConstants.getValue(HTTP_REQUEST_METHOD),
                        AttributeValue.newBuilder().setValue("GET").build(),
                        RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD),
                        AttributeValue.newBuilder().setValue("PUT").build()))
                .build());
    assertEquals(Optional.of("GET"), HttpSemanticConventionUtils.getHttpMethod(event));
  }

  @Test
  public void testGetHttpScheme() {
    Event event =
        createMockEventWithAttribute(OTelHttpSemanticConventions.HTTP_SCHEME.getValue(), "https");
    assertEquals(Optional.of("https"), HttpSemanticConventionUtils.getHttpScheme(event));

    event = createMockEventWithAttribute(OTelHttpSemanticConventions.HTTP_SCHEME.getValue(), "");
    assertTrue(HttpSemanticConventionUtils.getHttpScheme(event).isEmpty());

    event =
        createMockEventWithAttribute(
            HttpSemanticConventions.HTTP_REQUEST_X_FORWARDED_PROTO.getValue(), "http");
    assertEquals(Optional.of("http"), HttpSemanticConventionUtils.getHttpScheme(event));

    event =
        createMockEventWithAttribute(
            HttpSemanticConventions.HTTP_REQUEST_FORWARDED.getValue(),
            "by=random;proto=https;for=modnar");
    assertEquals(Optional.of("https"), HttpSemanticConventionUtils.getHttpScheme(event));

    event =
        createMockEventWithAttribute(
            HttpSemanticConventions.HTTP_REQUEST_FORWARDED.getValue(),
            "by= random ; proto = https; for=modnar");
    assertEquals(Optional.of("https"), HttpSemanticConventionUtils.getHttpScheme(event));

    event =
        createMockEventWithAttribute(
            HttpSemanticConventions.HTTP_REQUEST_FORWARDED.getValue(), "by= random ; for=modnar");
    assertEquals(Optional.empty(), HttpSemanticConventionUtils.getHttpScheme(event));

    Event e = mock(Event.class);
    when(e.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        HTTP_SCHEME.getValue(),
                        AttributeValue.newBuilder().setValue("http").build(),
                        HttpSemanticConventions.HTTP_REQUEST_X_FORWARDED_PROTO.getValue(),
                        AttributeValue.newBuilder().setValue("https").build(),
                        HttpSemanticConventions.HTTP_REQUEST_FORWARDED.getValue(),
                        AttributeValue.newBuilder()
                            .setValue("by= random ;proto = ; for=modnar")
                            .build()))
                .build());
    when(e.getEnrichedAttributes()).thenReturn(null);
    assertEquals(Optional.of("https"), HttpSemanticConventionUtils.getHttpScheme(e));

    // when only origin header exists expect whatever is the scheme of the origin header
    // when origin header has 'https'
    event =
        createMockEventWithAttribute(
            HttpSemanticConventions.HTTP_REQUEST_ORIGIN.getValue(), "https://abc.xyz");
    assertEquals(Optional.of("https"), HttpSemanticConventionUtils.getHttpScheme(event));

    // when origin header has 'http'
    event =
        createMockEventWithAttribute(
            HttpSemanticConventions.HTTP_REQUEST_ORIGIN.getValue(), "http://abc.xyz");
    assertEquals(Optional.of("http"), HttpSemanticConventionUtils.getHttpScheme(event));

    // when http url and origin header exists with scheme https then expect https
    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        HttpSemanticConventions.HTTP_REQUEST_ORIGIN.getValue(),
                        AttributeValue.newBuilder().setValue("https://abc.xyz.ai").build(),
                        RawSpanConstants.getValue(Http.HTTP_URL),
                        AttributeValue.newBuilder()
                            .setValue("http://abc.xyz.ai/apis/5673/events?a1=v1&a2=v2")
                            .build()))
                .build());
    assertEquals(Optional.of("https"), HttpSemanticConventionUtils.getHttpScheme(event));

    // when http url and origin header exists with scheme http then expect http
    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        HttpSemanticConventions.HTTP_REQUEST_ORIGIN.getValue(),
                        AttributeValue.newBuilder().setValue("http://abc.xyz.ai").build(),
                        RawSpanConstants.getValue(Http.HTTP_URL),
                        AttributeValue.newBuilder()
                            .setValue("http://abc.xyz.ai/apis/5673/events?a1=v1&a2=v2")
                            .build()))
                .build());
    assertEquals(Optional.of("http"), HttpSemanticConventionUtils.getHttpScheme(event));

    // when http url and origin header is null string then expect scheme of the url
    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        HttpSemanticConventions.HTTP_REQUEST_ORIGIN.getValue(),
                        AttributeValue.newBuilder().setValue("null").build(),
                        RawSpanConstants.getValue(Http.HTTP_URL),
                        AttributeValue.newBuilder()
                            .setValue("http://abc.xyz.ai/apis/5673/events?a1=v1&a2=v2")
                            .build()))
                .build());
    assertEquals(Optional.of("http"), HttpSemanticConventionUtils.getHttpScheme(event));
  }

  @Test
  public void testGetHttpUrl() {
    Event event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(Http.HTTP_URL),
            "https://example.ai/apis/5673/events?a1=v1&a2=v2");
    assertEquals(
        Optional.of("https://example.ai/apis/5673/events?a1=v1&a2=v2"),
        HttpSemanticConventionUtils.getHttpUrl(event));

    event = mock(Event.class);
    assertTrue(HttpSemanticConventionUtils.getHttpUrl(event).isEmpty());

    event = createMockEventWithAttribute(RawSpanConstants.getValue(Http.HTTP_URL), "");
    assertTrue(HttpSemanticConventionUtils.getHttpPath(event).isEmpty());

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL),
                        AttributeValue.newBuilder()
                            .setValue("https://example.ai/apis/5673/events?a1=v1&a2=v2")
                            .build(),
                        RawSpanConstants.getValue(HTTP_REQUEST_URL),
                        AttributeValue.newBuilder()
                            .setValue("https://example2.ai/apis/5673/events?a1=v1&a2=v2")
                            .build(),
                        RawSpanConstants.getValue(Http.HTTP_URL),
                        AttributeValue.newBuilder()
                            .setValue("https://example4.ai/apis/5673/events?a1=v1&a2=v2")
                            .build()))
                .build());
    assertEquals(
        Optional.of("https://example.ai/apis/5673/events?a1=v1&a2=v2"),
        HttpSemanticConventionUtils.getHttpUrl(event));
  }

  @Test
  public void testGetHttpQueryString() {
    Event event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING), "a1=v1&a2=v2");
    assertEquals(Optional.of("a1=v1&a2=v2"), HttpSemanticConventionUtils.getHttpQueryString(event));
  }

  @Test
  public void testGetHttpRequestHeaderPath() {
    Event event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(HTTP_REQUEST_HEADER_PATH), "sample/http/request/header/path");
    assertEquals(
        Optional.of("sample/http/request/header/path"),
        HttpSemanticConventionUtils.getHttpRequestHeaderPath(event));
  }

  @Test
  public void testGetHttpXForwardedFor() {
    Event event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(HTTP_REQUEST_X_FORWARDED_FOR_HEADER),
            "forwarded for header val");
    assertEquals(
        Optional.of("forwarded for header val"),
        HttpSemanticConventionUtils.getHttpXForwardedFor(event));
  }

  @Test
  public void testGetHttpRequestContentType() {
    Event event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(HTTP_REQUEST_CONTENT_TYPE), "application/text");
    assertEquals(
        Optional.of("application/text"),
        HttpSemanticConventionUtils.getHttpRequestContentType(event));
  }

  @Test
  public void testGetHttpRequestSize() {
    Event event = createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_REQUEST_SIZE), "100");
    assertEquals(Optional.of(100), HttpSemanticConventionUtils.getHttpRequestSize(event));

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RawSpanConstants.getValue(ENVOY_REQUEST_SIZE),
                        AttributeValue.newBuilder().setValue("100").build(),
                        OTelHttpSemanticConventions.HTTP_REQUEST_SIZE.getValue(),
                        AttributeValue.newBuilder().setValue("150").build(),
                        RawSpanConstants.getValue(HTTP_REQUEST_SIZE),
                        AttributeValue.newBuilder().setValue("200").build(),
                        RawSpanConstants.getValue(HTTP_REQUEST_CONTENT_LENGTH),
                        AttributeValue.newBuilder().setValue("300").build(),
                        RawSpanConstants.getValue(HTTP_HTTP_REQUEST_BODY),
                        AttributeValue.newBuilder().setValue("Hello, there!").build()))
                .build());
    assertEquals(Optional.of(100), HttpSemanticConventionUtils.getHttpRequestSize(event));

    event =
        createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_REQUEST_CONTENT_LENGTH), "300");
    assertEquals(Optional.of(300), HttpSemanticConventionUtils.getHttpRequestSize(event));

    event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(HTTP_HTTP_REQUEST_BODY), "Hello, there!");
    assertEquals(Optional.of(13), HttpSemanticConventionUtils.getHttpRequestSize(event));

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RawSpanConstants.getValue(HTTP_HTTP_REQUEST_BODY),
                        AttributeValue.newBuilder().setValue("Hello, there!").build(),
                        RawSpanConstants.getValue(HTTP_REQUEST_BODY_TRUNCATED),
                        AttributeValue.newBuilder().setValue("true").build()))
                .build());
    assertEquals(Optional.empty(), HttpSemanticConventionUtils.getHttpRequestSize(event));
  }

  @Test
  public void testGetHttpResponseSize() {
    Event event =
        createMockEventWithAttribute(RawSpanConstants.getValue(HTTP_RESPONSE_SIZE), "100");
    assertEquals(Optional.of(100), HttpSemanticConventionUtils.getHttpResponseSize(event));

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE),
                        AttributeValue.newBuilder().setValue("100").build(),
                        RawSpanConstants.getValue(HTTP_RESPONSE_SIZE),
                        AttributeValue.newBuilder().setValue("150").build(),
                        OTelHttpSemanticConventions.HTTP_RESPONSE_SIZE.getValue(),
                        AttributeValue.newBuilder().setValue("200").build(),
                        RawSpanConstants.getValue(HTTP_RESPONSE_CONTENT_LENGTH),
                        AttributeValue.newBuilder().setValue("300").build(),
                        RawSpanConstants.getValue(HTTP_HTTP_RESPONSE_BODY),
                        AttributeValue.newBuilder().setValue("Hello World!").build()))
                .build());
    assertEquals(Optional.of(100), HttpSemanticConventionUtils.getHttpResponseSize(event));

    event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(HTTP_RESPONSE_CONTENT_LENGTH), "300");
    assertEquals(Optional.of(300), HttpSemanticConventionUtils.getHttpResponseSize(event));

    event =
        createMockEventWithAttribute(
            RawSpanConstants.getValue(HTTP_HTTP_RESPONSE_BODY), "Hello World!");
    assertEquals(Optional.of(12), HttpSemanticConventionUtils.getHttpResponseSize(event));

    event = mock(Event.class);
    when(event.getAttributes())
        .thenReturn(
            Attributes.newBuilder()
                .setAttributeMap(
                    Map.of(
                        RawSpanConstants.getValue(HTTP_HTTP_RESPONSE_BODY),
                        AttributeValue.newBuilder().setValue("Hello World!").build(),
                        RawSpanConstants.getValue(HTTP_RESPONSE_BODY_TRUNCATED),
                        AttributeValue.newBuilder().setValue("true").build()))
                .build());
    assertEquals(Optional.empty(), HttpSemanticConventionUtils.getHttpResponseSize(event));
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
}
