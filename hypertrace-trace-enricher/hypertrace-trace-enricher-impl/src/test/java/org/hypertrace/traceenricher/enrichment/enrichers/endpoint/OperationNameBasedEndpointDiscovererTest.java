package org.hypertrace.traceenricher.enrichment.enrichers.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.entity.data.service.v1.Entity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OperationNameBasedEndpointDiscovererTest {

  private OperationNameBasedEndpointDiscoverer endpointDiscoverer;
  private ApiEntityDao apiEntityDao;

  @BeforeEach
  public void setup() {
    apiEntityDao = mock(ApiEntityDao.class);
    endpointDiscoverer =
        new OperationNameBasedEndpointDiscoverer("tenant-1", "service-1", "service1", apiEntityDao);
  }

  @Test
  public void whenCacheIsEmptyExpectCacheToLoadAndReturnEntity() throws Exception {
    Entity entity =
        Entity.newBuilder().setEntityId("entity-id").setEntityName("entity-name").build();
    when(apiEntityDao.upsertApiEntity(
            "tenant-1", "service-1", "service1", ApiEntityDao.API_TYPE, "Driver::getCustomers"))
        .thenReturn(entity);
    Event event =
        Event.newBuilder()
            .setEventId(ByteBuffer.wrap("event-id".getBytes()))
            .setEventName("Driver::getCustomers")
            .setCustomerId("tenant-1")
            .build();
    Entity expected = endpointDiscoverer.getApiEntity(event);
    assertEquals(
        expected, endpointDiscoverer.getPatternToApiEntityCache().get(event.getEventName()));
  }

  @Test
  public void whenCacheIsNotEmptyExpectCacheToReturnCachedEntity() throws Exception {
    Entity entity =
        Entity.newBuilder().setEntityId("entity-id").setEntityName("entity-name").build();
    when(apiEntityDao.upsertApiEntity(
            "tenant-1", "service-1", "service1", ApiEntityDao.API_TYPE, "Driver::getCustomers"))
        .thenReturn(entity);
    Event event =
        Event.newBuilder()
            .setEventId(ByteBuffer.wrap("event-id".getBytes()))
            .setEventName("Driver::getCustomers")
            .setCustomerId("tenant-1")
            .build();
    Entity expected = endpointDiscoverer.getApiEntity(event);

    // query again
    endpointDiscoverer.getApiEntity(event);
    // make sure cache didn't trigger a load again. dao should have been called only once during
    // setup above
    verify(apiEntityDao, times(1))
        .upsertApiEntity(
            "tenant-1", "service-1", "service1", ApiEntityDao.API_TYPE, event.getEventName());
  }
}
