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

  private OperationNameBasedEndpointDiscoverer underTest;
  private ApiEntityDao dao;

  @BeforeEach
  public void setup() {
    dao = mock(ApiEntityDao.class);
    underTest = new OperationNameBasedEndpointDiscoverer("tenant-1", "service-1", dao);
  }

  @Test
  public void whenCacheIsEmptyExpectCacheToLoadAndReturnEntity() throws Exception {
    Entity entity =
        Entity.newBuilder().setEntityId("entity-id").setEntityName("entity-name").build();
    when(dao.upsertApiEntity(
            "tenant-1", "service-1", ApiEntityDao.API_TYPE, "Driver::getCustomers"))
        .thenReturn(entity);
    Event event =
        Event.newBuilder()
            .setEventId(ByteBuffer.wrap("event-id".getBytes()))
            .setEventName("Driver::getCustomers")
            .setCustomerId("tenant-1")
            .build();
    Entity expected = underTest.getApiEntity(event);
    assertEquals(expected, underTest.getPatternToApiEntityCache().get(event.getEventName()));
  }

  @Test
  public void whenCacheIsNotEmptyExpectCacheToReturnCachedEntity() throws Exception {
    Entity entity =
        Entity.newBuilder().setEntityId("entity-id").setEntityName("entity-name").build();
    when(dao.upsertApiEntity(
            "tenant-1", "service-1", ApiEntityDao.API_TYPE, "Driver::getCustomers"))
        .thenReturn(entity);
    Event event =
        Event.newBuilder()
            .setEventId(ByteBuffer.wrap("event-id".getBytes()))
            .setEventName("Driver::getCustomers")
            .setCustomerId("tenant-1")
            .build();
    Entity expected = underTest.getApiEntity(event);

    // query again
    underTest.getApiEntity(event);
    // make sure cache didn't trigger a load again. dao should have been called only once during
    // setup above
    verify(dao, times(1))
        .upsertApiEntity("tenant-1", "service-1", ApiEntityDao.API_TYPE, event.getEventName());
  }
}
