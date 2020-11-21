package org.hypertrace.attributeutils.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hypertrace.attributeutils.http.HttpCodeMapper;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ApiStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.engine.Constants;

public class DefaultHttpCodeMapperTest {

  @Test
  public void test_getMessage_validCode_shouldGetMessage() {
    assertEquals("Unauthorized", HttpCodeMapper.getMessage("401"));
  }

  @Test
  public void test_getMessage_invalidCode_shouldGetNull() {
    assertNull(HttpCodeMapper.getMessage("0"));
  }

  @Test
  public void test_getMessage_nullCode_shouldGetNull() {
    assertNull(HttpCodeMapper.getMessage(null));
  }

  @Test
  public void test_getState_nullCode_shouldGetNull() {
    assertNull(HttpCodeMapper.getMessage(null));
  }

  @Test
  public void test_getState_successCode_shouldGetSuccess() {
    Assertions.assertEquals(EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_SUCCESS),
        HttpCodeMapper.getState("200"));
    Assertions.assertEquals(EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_SUCCESS),
        HttpCodeMapper.getState("300"));
  }

  @Test
  public void test_getState_failCode_shouldGetFail() {
    Assertions.assertEquals(EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_FAIL),
        HttpCodeMapper.getState("400"));
    Assertions.assertEquals(EnrichedSpanConstants.getValue(ApiStatus.API_STATUS_FAIL),
        HttpCodeMapper.getState("500"));
  }
}
