package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.core.span.constants.v1.Http.HTTP_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_QUERY_STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Http;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class HttpAttributeEnricherTest extends AbstractAttributeEnricherTest {

  private static final Http HTTP_REQUEST_PATH = Http.HTTP_REQUEST_PATH;
  private static final Http HTTP_REQUEST_QUERY_PARAM = Http.HTTP_REQUEST_QUERY_PARAM;
  private static final org.hypertrace.core.span.constants.v1.Http HTTP_REQUEST_URL =
      org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;

  @Mock private StructuredTrace mockTrace;

  private final HttpAttributeEnricher enricher = new HttpAttributeEnricher();

  @Test
  public void test_withAValidUrl_shouldEnrichHttpPathAndParams() {
    Event e = createMockEvent();
    String query_string = "action=checkout&age=23&location=";
    addAttribute(e, RawSpanConstants.getValue(HTTP_PATH), "/users");
    addAttribute(e, RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING), query_string);

    enricher.enrichEvent(mockTrace, e);

    String httpPathEnrichedValue =
        SpanAttributeUtils.getStringAttribute(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_PATH));
    assertEquals("/users", httpPathEnrichedValue);

    String actionParam =
        SpanAttributeUtils.getStringAttribute(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("checkout", actionParam);

    String ageParam =
        SpanAttributeUtils.getStringAttribute(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".age");
    assertEquals("23", ageParam);

    String locationParam =
        SpanAttributeUtils.getStringAttribute(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".location");
    assertNull(locationParam);
  }

  @Test
  public void testMultipleValuedQueryParam() {
    String query_string = "action=checkout&action=a&age=2&age=3&location=";
    Event e = createMockEvent();
    addAttribute(e, RawSpanConstants.getValue(HTTP_PATH), "/users");
    addAttribute(e, RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING), query_string);
    enricher.enrichEvent(mockTrace, e);

    String httpPathEnrichedValue =
        SpanAttributeUtils.getStringAttribute(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_PATH));
    assertEquals("/users", httpPathEnrichedValue);

    AttributeValue actionParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("checkout", actionParam.getValue());
    assertEquals(List.of("checkout", "a"), actionParam.getValueList());

    AttributeValue ageParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".age");
    assertEquals("2", ageParam.getValue());
    assertEquals(List.of("2", "3"), ageParam.getValueList());

    AttributeValue locationParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".location");
    assertNull(locationParam);
  }

  @Test
  public void testGetQueryParamsFromUrl() {
    String query_string = "action=checkout&cat=1dog=2&action=a&age=2&age=3;&location=&area&&";
    Event e = createMockEvent();
    // ; in url should not be treated as an splitting character.
    // Url in this test also contains successive & and param with no value.
    addAttribute(e, RawSpanConstants.getValue(HTTP_PATH), "/users");
    addAttribute(e, RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING), query_string);

    enricher.enrichEvent(mockTrace, e);

    String httpPathEnrichedValue =
        SpanAttributeUtils.getStringAttribute(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_PATH));
    assertEquals("/users", httpPathEnrichedValue);

    AttributeValue actionParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("checkout", actionParam.getValue());
    assertEquals(List.of("checkout", "a"), actionParam.getValueList());

    AttributeValue ageParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".age");
    assertEquals("2", ageParam.getValue());
    assertEquals(List.of("2", "3;"), ageParam.getValueList());

    AttributeValue locationParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".location");
    assertNull(locationParam);

    AttributeValue areaParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".area");
    assertNull(areaParam);

    AttributeValue catParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".cat");
    assertEquals("1dog=2", catParam.getValue());
    assertEquals(List.of("1dog=2"), catParam.getValueList());
  }

  @Test
  public void testSemicolonInQueryParam() {
    String query_string = "action=checkout;age=2";
    Event e = createMockEvent();
    addAttribute(e, RawSpanConstants.getValue(HTTP_PATH), "/users");
    addAttribute(e, RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING), query_string);

    enricher.enrichEvent(mockTrace, e);

    AttributeValue actionParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("checkout;age=2", actionParam.getValue());
    assertEquals(List.of("checkout;age=2"), actionParam.getValueList());
  }

  @Test
  public void testDecodeQueryParams() {
    String query_string = "action=check%20out&location=hello%3Dworld";
    Event e = createMockEvent();
    addAttribute(e, RawSpanConstants.getValue(HTTP_PATH), "/users");
    addAttribute(e, RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING), query_string);

    enricher.enrichEvent(mockTrace, e);

    AttributeValue actionParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("check out", actionParam.getValue());
    assertEquals(List.of("check out"), actionParam.getValueList());

    AttributeValue locationParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".location");
    assertEquals("hello=world", locationParam.getValue());
    assertEquals(List.of("hello=world"), locationParam.getValueList());
  }

  @Test
  public void testDecodeQueryParamsInvalidInput() {
    // Try putting invalid encoded string in both query param key and value.
    String query_string = "action=check%!mout&loca%.Ption=hello%3Dworld";
    Event e = createMockEvent();
    addAttribute(e, RawSpanConstants.getValue(HTTP_PATH), "/users");
    addAttribute(e, RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING), query_string);

    enricher.enrichEvent(mockTrace, e);

    // If input is invalid(can't be decoded) the input is returned as it is.
    AttributeValue actionParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("check%!mout", actionParam.getValue());
    assertEquals(List.of("check%!mout"), actionParam.getValueList());

    AttributeValue locationParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".loca%.Ption");
    assertEquals("hello=world", locationParam.getValue());
    assertEquals(List.of("hello=world"), locationParam.getValueList());
  }

  @Test
  public void testDecodeQueryParamsWithSquareBrackets() {
    // Url with query params not encoded
    String query_string = "action[]=checkout&age=2&[]=test";
    Event e = createMockEvent();
    addAttribute(e, RawSpanConstants.getValue(HTTP_PATH), "/users");
    addAttribute(e, RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING), query_string);

    enricher.enrichEvent(mockTrace, e);

    AttributeValue actionParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".action");
    assertEquals("checkout", actionParam.getValue());
    assertEquals(List.of("checkout"), actionParam.getValueList());

    AttributeValue ageParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".age");
    assertEquals("2", ageParam.getValue());
    assertEquals(List.of("2"), ageParam.getValueList());

    // If seen only bracket, treat the square braces as param key
    AttributeValue onlySquareBracketParam =
        SpanAttributeUtils.getAttributeValue(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_QUERY_PARAM) + ".[]");
    assertEquals("test", onlySquareBracketParam.getValue());
    assertEquals(List.of("test"), onlySquareBracketParam.getValueList());

    // Create event with URL encoded query params
    query_string = "action%5B%5D%3Dcheckout%26age%3D2%26%5B%5D%3Dtest";
    e = createMockEvent();
    addAttribute(e, RawSpanConstants.getValue(HTTP_PATH), "/users");
    addAttribute(e, RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING), query_string);

    enricher.enrichEvent(mockTrace, e);
    assertEquals("checkout", actionParam.getValue());
    assertEquals(List.of("checkout"), actionParam.getValueList());

    assertEquals("2", ageParam.getValue());
    assertEquals(List.of("2"), ageParam.getValueList());

    assertEquals("test", onlySquareBracketParam.getValue());
    assertEquals(List.of("test"), onlySquareBracketParam.getValueList());
  }

  @Test
  public void test_withNoQueryParams_shouldOnlyEnrichPath() {
    Event e = createMockEvent();
    addAttribute(e, RawSpanConstants.getValue(HTTP_PATH), "/users");

    enricher.enrichEvent(mockTrace, e);

    String httpPathEnrichedValue =
        SpanAttributeUtils.getStringAttribute(
            e, Constants.getEnrichedSpanConstant(HTTP_REQUEST_PATH));
    assertEquals("/users", httpPathEnrichedValue);
    assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
  }

  private void addAttribute(Event event, String key, String val) {
    event
        .getAttributes()
        .getAttributeMap()
        .put(key, AttributeValue.newBuilder().setValue(val).build());
  }
}
