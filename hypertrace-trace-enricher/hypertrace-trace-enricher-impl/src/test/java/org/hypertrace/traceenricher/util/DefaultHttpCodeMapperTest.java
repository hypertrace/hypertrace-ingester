package org.hypertrace.traceenricher.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hypertrace.traceenricher.enrichedspan.constants.v1.ApiStatus;
import org.junit.jupiter.api.Test;

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
    assertEquals(
        Constants.getEnrichedSpanConstant(ApiStatus.API_STATUS_SUCCESS),
        HttpCodeMapper.getState("200"));
    assertEquals(
        Constants.getEnrichedSpanConstant(ApiStatus.API_STATUS_SUCCESS),
        HttpCodeMapper.getState("300"));
  }

  @Test
  public void test_getState_failCode_shouldGetFail() {
    assertEquals(
        Constants.getEnrichedSpanConstant(ApiStatus.API_STATUS_FAIL),
        HttpCodeMapper.getState("400"));
    assertEquals(
        Constants.getEnrichedSpanConstant(ApiStatus.API_STATUS_FAIL),
        HttpCodeMapper.getState("500"));
  }
}
