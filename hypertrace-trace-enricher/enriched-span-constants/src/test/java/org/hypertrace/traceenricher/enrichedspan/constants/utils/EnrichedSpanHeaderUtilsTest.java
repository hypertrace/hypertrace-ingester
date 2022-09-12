package org.hypertrace.traceenricher.enrichedspan.constants.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.hypertrace.core.datamodel.Event;
import org.junit.jupiter.api.Test;

public class EnrichedSpanHeaderUtilsTest {
  @Test
  public void testGetRequestHeadersExceptCookies() {
    Map<String, String> spanRequestHeadersExceptCookies =
        Map.of(
            "sample-security-type",
            "sample-security-val",
            "sample-security-key",
            "sample-security-val");
    Event e = EnrichedSpanUtilsTest.createMockEventWithHeadersAndCookies();
    assertEquals(
        spanRequestHeadersExceptCookies, EnrichedSpanHeaderUtils.getRequestHeadersExceptCookies(e));
  }

  @Test
  public void testGetResponseHeadersExceptCookies() {
    Map<String, String> spanResponseHeadersExceptCookies =
        Map.of(
            "sample-security-type",
            "sample-security-val",
            "sample-security-key",
            "sample-security-val");
    Event e = EnrichedSpanUtilsTest.createMockEventWithHeadersAndCookies();
    assertEquals(
        spanResponseHeadersExceptCookies,
        EnrichedSpanHeaderUtils.getResponseHeadersExceptCookies(e));
  }
}
