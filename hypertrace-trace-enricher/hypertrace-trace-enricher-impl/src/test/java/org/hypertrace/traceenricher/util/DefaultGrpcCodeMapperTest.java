package org.hypertrace.traceenricher.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hypertrace.traceenricher.enrichedspan.constants.v1.ApiStatus;
import org.junit.jupiter.api.Test;

public class DefaultGrpcCodeMapperTest {

  @Test
  public void test_getMessage_validCode_shouldGetMessage() {
    assertEquals("OK", GrpcCodeMapper.getMessage("0"));
  }

  @Test
  public void test_getMessage_invalidCode_shouldGetUnknown() {
    assertEquals("UNKNOWN", GrpcCodeMapper.getMessage("900"));
  }

  @Test
  public void test_getMessage_nullCode_shouldGetNull() {
    assertNull(GrpcCodeMapper.getMessage(null));
  }

  @Test
  public void test_getState_nullCode_shouldGetNull() {
    assertNull(GrpcCodeMapper.getMessage(null));
  }

  @Test
  public void test_getState_successCode_shouldGetSuccess() {
    assertEquals(
        Constants.getEnrichedSpanConstant(ApiStatus.API_STATUS_SUCCESS),
        GrpcCodeMapper.getState("0"));
  }

  @Test
  public void test_getState_failCode_shouldGetFail() {
    assertEquals(
        Constants.getEnrichedSpanConstant(ApiStatus.API_STATUS_FAIL), GrpcCodeMapper.getState("5"));
  }
}
