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

  private HttpAttributeEnricher enricher = new HttpAttributeEnricher();

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
