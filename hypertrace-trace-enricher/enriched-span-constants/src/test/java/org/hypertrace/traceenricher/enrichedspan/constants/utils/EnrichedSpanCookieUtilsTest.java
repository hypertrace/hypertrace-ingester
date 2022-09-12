package org.hypertrace.traceenricher.enrichedspan.constants.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.hypertrace.core.datamodel.Event;
import org.junit.jupiter.api.Test;

public class EnrichedSpanCookieUtilsTest {
  @Test
  public void testGetRequestCookies() {
    Map<String, String> spanRequestCookies =
        Map.of("name", "sample-cookie-name", "token", "sample-token-val");
    Event e = EnrichedSpanUtilsTest.createMockEventWithHeadersAndCookies();
    assertEquals(spanRequestCookies, EnrichedSpanCookieUtils.getRequestCookies(e));
  }

  @Test
  public void testGetResponseCookies() {
    Map<String, String> spanResponseCookies = Map.of("name", "sample-cookie-name");
    Event e = EnrichedSpanUtilsTest.createMockEventWithHeadersAndCookies();
    assertEquals(spanResponseCookies, EnrichedSpanCookieUtils.getResponseCookies(e));
  }
}
