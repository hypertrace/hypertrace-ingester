package org.hypertrace.traceenricher.enrichment.enrichers.endpoint;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.hypertrace.entity.data.service.client.EdsClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiEntityDaoTest {

  private EdsClient edsClient;
  private ApiEntityDao apiEntityDao;

  @BeforeEach
  void setup() {
    edsClient = mock(EdsClient.class);
    apiEntityDao = new ApiEntityDao(edsClient);
  }

  @Test
  void whenValidDataIsPassedExpectDaoToBeCalled() {
    apiEntityDao.upsertApiEntity(
        "tenant-1", "service-1", "service1", "OPERATION_NAME", "Driver:;getCustomers");
    verify(edsClient).upsert(any(org.hypertrace.entity.data.service.v1.Entity.class));
  }

  @Test
  void whenParamsAreNullThenExpectNullPointerException() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          apiEntityDao.upsertApiEntity(
              null, "service-1", "service1", "OPERATION_NAME", "Driver:;getCustomers");
        });
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          apiEntityDao.upsertApiEntity(
              "tenant-1", null, null, "OPERATION_NAME", "Driver:;getCustomers");
        });
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          apiEntityDao.upsertApiEntity("tenant-1", "service-1", null, null, "Driver:;getCustomers");
        });
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          apiEntityDao.upsertApiEntity(
              "tenant-1", "service-1", "service1", null, "Driver:;getCustomers");
        });
    Assertions.assertThrows(
        NullPointerException.class,
        () -> {
          apiEntityDao.upsertApiEntity("tenant-1", "service-1", "service1", "OPERATION_NAME", null);
        });
  }
}
