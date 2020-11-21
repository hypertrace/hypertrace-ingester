package org.hypertrace.telemetry.attribute.utils.http;

import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ApiStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link HttpCodeMapper}
 */
public class HttpCodeMapperTest {

  @Test
  public void testHttpCodeMapper() {

    // Verify Status Message.
    Assertions.assertEquals("OK", HttpCodeMapper.getMessage("200"));
    Assertions.assertEquals("Not Modified", HttpCodeMapper.getMessage("304"));
    Assertions.assertEquals("Unauthorized", HttpCodeMapper.getMessage("401"));
    Assertions.assertEquals("Not Extended", HttpCodeMapper.getMessage("510"));
    // Not mapping informational 1xx
    Assertions.assertEquals(null, HttpCodeMapper.getMessage("101"));
    Assertions.assertEquals(null, HttpCodeMapper.getMessage(null));

    // Verify state.
    Assertions.assertEquals(EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_SUCCESS), HttpCodeMapper.getState("202"));
    Assertions.assertEquals(EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_SUCCESS), HttpCodeMapper.getState("302"));
    Assertions.assertEquals(EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_FAIL), HttpCodeMapper.getState("401"));
    Assertions.assertEquals(EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_FAIL), HttpCodeMapper.getState("500"));
    Assertions.assertEquals(null, HttpCodeMapper.getState(null));
  }
}
