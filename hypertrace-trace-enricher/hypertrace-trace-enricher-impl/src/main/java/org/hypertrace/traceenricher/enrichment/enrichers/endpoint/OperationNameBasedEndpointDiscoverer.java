package org.hypertrace.traceenricher.enrichment.enrichers.endpoint;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.entity.data.service.v1.Entity;

/**
 * Extracts the endpoint name which is the operation_name tag on a span and creates an API Entity
 * for that. It also caches the entity
 */
public class OperationNameBasedEndpointDiscoverer {

  private final String customerId;
  private final String serviceId;
  private final ApiEntityDao apiEntityDao;

  private final LoadingCache<String, Entity> patternToApiEntityCache =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .expireAfterWrite(5, TimeUnit.MINUTES)
          .build(
              new CacheLoader<>() {
                public Entity load(@Nonnull String pattern) {
                  return getEntityForPattern(pattern);
                }
              });

  public OperationNameBasedEndpointDiscoverer(
      String customerId, String serviceId, ApiEntityDao apiEntityDao) {
    this.customerId = customerId;
    this.serviceId = serviceId;
    this.apiEntityDao = apiEntityDao;
  }

  public Entity getApiEntity(Event spanEvent) throws ExecutionException {
    String name = spanEvent.getEventName();
    return patternToApiEntityCache.get(name);
  }

  private Entity getEntityForPattern(String pattern) {
    return apiEntityDao.upsertApiEntity(customerId, serviceId, ApiEntityDao.API_TYPE, pattern);
  }

  @VisibleForTesting
  LoadingCache<String, Entity> getPatternToApiEntityCache() {
    return patternToApiEntityCache;
  }
}
