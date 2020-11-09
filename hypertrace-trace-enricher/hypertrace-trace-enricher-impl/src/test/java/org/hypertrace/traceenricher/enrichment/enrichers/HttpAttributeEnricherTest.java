package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Http;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class HttpAttributeEnricherTest extends AbstractAttributeEnricherTest {

  private static final Http HTTP_REQUEST_PATH = Http.HTTP_REQUEST_PATH;
  private static final Http HTTP_REQUEST_QUERY_PARAM = Http.HTTP_REQUEST_QUERY_PARAM;
  private static final org.hypertrace.core.span.constants.v1.Http HTTP_REQUEST_URL =
      org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;

  @Mock
  private StructuredTrace mockTrace;

  private final HttpAttributeEnricher enricher = new HttpAttributeEnricher();

  @Test
  public void test_withAValidUrl_shouldEnrichHttpPathAndParams() {
    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder().setRequest(
            Request.newBuilder()
                .setUrl("http://hypertrace.org/users?action=checkout&age=23&location=").build())
            .build());

    enricher.enrichEvent(mockTrace, e);

    String httpPathEnrichedValue = SpanAttributeUtils.getStringAttribute(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_PATH));
    assertEquals("/users", httpPathEnrichedValue);

    String actionParam = SpanAttributeUtils.getStringAttribute(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("checkout", actionParam);

    String ageParam = SpanAttributeUtils.getStringAttribute(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".age");
    assertEquals("23", ageParam);

    String locationParam = SpanAttributeUtils.getStringAttribute(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".location");
    assertEquals("", locationParam);
  }

  @Test
  public void testMultipleValuedQueryParam() {
    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder().setRequest(
            Request.newBuilder()
                .setUrl("http://hypertrace.org/users?action=checkout&action=a&age=2&age=3&location=")
                .build())
            .build());
    enricher.enrichEvent(mockTrace, e);

    String httpPathEnrichedValue = SpanAttributeUtils.getStringAttribute(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_PATH));
    assertEquals("/users", httpPathEnrichedValue);

    AttributeValue actionParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("checkout", actionParam.getValue());
    assertEquals(List.of("checkout", "a"), actionParam.getValueList());

    AttributeValue ageParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".age");
    assertEquals("2", ageParam.getValue());
    assertEquals(List.of("2", "3"), ageParam.getValueList());

    AttributeValue locationParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".location");
    assertEquals("", locationParam.getValue());
    assertEquals(List.of(""), locationParam.getValueList());
  }

  @Test
  public void testGetQueryParamsFromUrl() {
    Event e = createMockEvent();
    // ; in url should not be treated as an splitting character.
    // Url in this test also contains successive & and param with no value.
    when(e.getHttp())
        .thenReturn(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder().setRequest(
            Request.newBuilder()
                .setUrl("http://hypertrace.org/users?action=checkout&cat=1dog=2&action=a&age=2&age=3;&location=&area&&")
                .build())
            .build());
    enricher.enrichEvent(mockTrace, e);

    String httpPathEnrichedValue = SpanAttributeUtils.getStringAttribute(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_PATH));
    assertEquals("/users", httpPathEnrichedValue);

    AttributeValue actionParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("checkout", actionParam.getValue());
    assertEquals(List.of("checkout", "a"), actionParam.getValueList());

    AttributeValue ageParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".age");
    assertEquals("2", ageParam.getValue());
    assertEquals(List.of("2", "3;"), ageParam.getValueList());

    AttributeValue locationParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".location");
    assertEquals("", locationParam.getValue());
    assertEquals(List.of(""), locationParam.getValueList());

    AttributeValue areaParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".area");
    assertEquals("", areaParam.getValue());
    assertEquals(List.of(""), areaParam.getValueList());

    AttributeValue catParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".cat");
    assertEquals("1dog=2", catParam.getValue());
    assertEquals(List.of("1dog=2"), catParam.getValueList());
  }

  @Test
  public void testSemicolonInQueryParam() {
    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder().setRequest(
            Request.newBuilder()
                .setUrl("http://hypertrace.org/users?action=checkout;age=2")
                .build())
            .build());
    enricher.enrichEvent(mockTrace, e);

    AttributeValue actionParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("checkout;age=2", actionParam.getValue());
    assertEquals(List.of("checkout;age=2"), actionParam.getValueList());
  }

  @Test
  public void testNoValueForQueryParam() {
    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder().setRequest(
            Request.newBuilder()
                .setUrl("http://hypertrace.org/users?action=checkout&location=&area&&")
                .build())
            .build());
    enricher.enrichEvent(mockTrace, e);

    AttributeValue actionParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("checkout", actionParam.getValue());
    assertEquals(List.of("checkout"), actionParam.getValueList());

    AttributeValue locationParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".location");
    assertEquals("", locationParam.getValue());
    assertEquals(List.of(""), locationParam.getValueList());

    AttributeValue areaParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".area");
    assertEquals("", areaParam.getValue());
    assertEquals(List.of(""), areaParam.getValueList());
  }

  @Test
  public void testDecodeQueryParams() {
    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder().setRequest(
            Request.newBuilder()
                .setUrl("http://hypertrace.org/users?action=check%20out&location=hello%3Dworld")
                .build())
            .build());
    enricher.enrichEvent(mockTrace, e);

    AttributeValue actionParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("check out", actionParam.getValue());
    assertEquals(List.of("check out"), actionParam.getValueList());

    AttributeValue locationParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".location");
    assertEquals("hello=world", locationParam.getValue());
    assertEquals(List.of("hello=world"), locationParam.getValueList());
  }

  @Test
  public void testDecodeQueryParamsInvalidInput() {
    //Try putting invalid encoded string in both query param key and value.
    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder().setRequest(
            Request.newBuilder()
                .setUrl("http://hypertrace.org/users?action=check%!mout&loca%.Ption=hello%3Dworld")
                .build())
            .build());
    enricher.enrichEvent(mockTrace, e);

    //If input is invalid(can't be decoded) the input is returned as it is.
    AttributeValue actionParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("check%!mout", actionParam.getValue());
    assertEquals(List.of("check%!mout"), actionParam.getValueList());

    AttributeValue locationParam = SpanAttributeUtils.getAttributeValue(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".loca%.Ption");
    assertEquals("hello=world", locationParam.getValue());
    assertEquals(List.of("hello=world"), locationParam.getValueList());
  }

  @Test
  public void test_withAnInvalidUrl_shouldSkipEnrichment() {
    Event e = createMockEvent();
    Map<String, AttributeValue> avm = e.getAttributes().getAttributeMap();
    avm.put(
        Constants.getRawSpanConstant(HTTP_REQUEST_URL),
        AttributeValue.newBuilder().setValue("/users").build()
    );

    enricher.enrichEvent(mockTrace, e);
    assertEquals(0, e.getEnrichedAttributes().getAttributeMap().size());
  }

  @Test
  public void test_withNoQueryParams_shouldOnlyEnrichPath() {
    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder().setRequest(
            Request.newBuilder().setUrl("http://hypertrace.org/users").build()).build());

    enricher.enrichEvent(mockTrace, e);

    String httpPathEnrichedValue = SpanAttributeUtils.getStringAttribute(e,
        Constants.getEnrichedSpanConstant(HTTP_REQUEST_PATH));
    assertEquals("/users", httpPathEnrichedValue);
    assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
  }
}
