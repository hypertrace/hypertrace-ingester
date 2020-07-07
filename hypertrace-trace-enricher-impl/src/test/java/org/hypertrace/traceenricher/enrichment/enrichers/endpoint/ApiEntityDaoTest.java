package org.hypertrace.traceenricher.enrichment.enrichers.endpoint;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.hypertrace.entity.data.service.client.EdsClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ApiEntityDaoTest {

  private EdsClient edsClient;
  private ApiEntityDao underTest;

  @BeforeEach
  public void setup() {
    edsClient = mock(EdsClient.class);
    underTest = new ApiEntityDao(edsClient);
  }

  @Test
  public void whenValidDataIsPassedExpectDaoToBeCalled() {
    underTest.upsertApiEntity("tenant-1", "service-1", "OPERATION_NAME", "Driver:;getCustomers");
    verify(edsClient).upsert(any(org.hypertrace.entity.data.service.v1.Entity.class));
  }

  @Test
  public void whenParamsAreNullThenExpectNullPointerException() {
    Assertions.assertThrows(NullPointerException.class, () -> {
      underTest.upsertApiEntity(null, "service-1", "OPERATION_NAME", "Driver:;getCustomers");
    });
    Assertions.assertThrows(NullPointerException.class, () -> {
      underTest.upsertApiEntity("tenant-1", null, "OPERATION_NAME", "Driver:;getCustomers");
    });
    Assertions.assertThrows(NullPointerException.class, () -> {
      underTest.upsertApiEntity("tenant-1", "service-1", null, "Driver:;getCustomers");
    });
    Assertions.assertThrows(NullPointerException.class, () -> {
      underTest.upsertApiEntity("tenant-1", "service-1", "OPERATION_NAME", null);
    });
  }
}