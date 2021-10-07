package org.hypertrace.traceenricher.enrichment.enrichers.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;

/** Class that holds all the entity related caches used by the enrichers */
public class EntityCache {
  private static final String DOT = ".";
  private final EdsClient edsClient;

  /**
   * Cache to cache the service fqn to service Entity mapping so that we don't look it up over and
   * over.
   */
  private final LoadingCache<Pair<String, String>, Optional<Entity>> fqnToServiceEntityCache;
  /**
   * Cache to cache the service name to a list of services mapping so that we don't look it up over
   * and over.
   */
  private final LoadingCache<Pair<String, String>, List<Entity>> nameToServiceEntitiesCache;

  /**
   * Cache of K8S namespaces Key: Customer Id, Namespace name Value: List of Namespace entity ids
   */
  private final LoadingCache<Pair<String, String>, List<Entity>> nameToNamespaceEntitiesCache;

  /**
   * Cache of Backend identifying attributes to Entity Key: Map of identifying attributes Value:
   * Optional Backend entity
   */
  private final LoadingCache<ContextualKey<Map<String, AttributeValue>>, Optional<Entity>>
      backendIdAttrsToEntityCache;

  public EntityCache(EdsClient edsClient, Executor asyncCacheLoaderExecutor) {
    this.edsClient = edsClient;
    fqnToServiceEntityCache =
        CacheBuilder.newBuilder()
            .maximumSize(10000)
            .refreshAfterWrite(4, TimeUnit.MINUTES)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::loadServiceFromFQN), asyncCacheLoaderExecutor));

    nameToServiceEntitiesCache =
        CacheBuilder.newBuilder()
            .maximumSize(10000)
            .refreshAfterWrite(4, TimeUnit.MINUTES)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(
                        tenantIdServiceNamePair ->
                            edsClient.getEntitiesByName(
                                tenantIdServiceNamePair.getLeft(),
                                EntityType.SERVICE.name(),
                                tenantIdServiceNamePair.getRight())),
                    asyncCacheLoaderExecutor));

    nameToNamespaceEntitiesCache =
        CacheBuilder.newBuilder()
            .maximumSize(10000)
            .refreshAfterWrite(4, TimeUnit.MINUTES)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(
                        key ->
                            edsClient.getEntitiesByName(
                                key.getLeft(), EntityType.K8S_NAMESPACE.name(), key.getRight())),
                    asyncCacheLoaderExecutor));

    backendIdAttrsToEntityCache =
        CacheBuilder.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::loadBackendFromIdentifyingAttributes),
                    asyncCacheLoaderExecutor));

    PlatformMetricsRegistry.registerCache(
        this.getClass().getName() + DOT + "fqnToServiceEntityCache",
        fqnToServiceEntityCache,
        Collections.emptyMap());
    PlatformMetricsRegistry.registerCache(
        this.getClass().getName() + DOT + "nameToServiceEntitiesCache",
        nameToServiceEntitiesCache,
        Collections.emptyMap());
    PlatformMetricsRegistry.registerCache(
        this.getClass().getName() + DOT + "nameToNamespaceEntitiesCache",
        nameToNamespaceEntitiesCache,
        Collections.emptyMap());
    PlatformMetricsRegistry.registerCache(
        this.getClass().getName() + DOT + "backendIdAttrsToEntityCache",
        backendIdAttrsToEntityCache,
        Collections.emptyMap());
  }

  public LoadingCache<Pair<String, String>, Optional<Entity>> getFqnToServiceEntityCache() {
    return fqnToServiceEntityCache;
  }

  public LoadingCache<Pair<String, String>, List<Entity>> getNameToServiceEntitiesCache() {
    return nameToServiceEntitiesCache;
  }

  public LoadingCache<Pair<String, String>, List<Entity>> getNameToNamespaceEntityIdCache() {
    return nameToNamespaceEntitiesCache;
  }

  public LoadingCache<ContextualKey<Map<String, AttributeValue>>, Optional<Entity>>
      getBackendIdAttrsToEntityCache() {
    return backendIdAttrsToEntityCache;
  }

  protected Optional<Entity> loadBackendFromIdentifyingAttributes(
      ContextualKey<Map<String, AttributeValue>> key) {
    ByTypeAndIdentifyingAttributes request =
        ByTypeAndIdentifyingAttributes.newBuilder()
            .setEntityType(EntityType.BACKEND.name())
            .putAllIdentifyingAttributes(key.getData())
            .build();
    return Optional.ofNullable(
        edsClient.getByTypeAndIdentifyingAttributes(
            key.getContext().getTenantId().orElseThrow(), request));
  }

  protected Optional<Entity> loadServiceFromFQN(Pair<String, String> tenantIdFQNPair) {
    AttributeValue fqnAttribute =
        AttributeValue.newBuilder()
            .setValue(Value.newBuilder().setString(tenantIdFQNPair.getRight()))
            .build();

    ByTypeAndIdentifyingAttributes request =
        ByTypeAndIdentifyingAttributes.newBuilder()
            .setEntityType(EntityType.SERVICE.name())
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN), fqnAttribute)
            .build();

    return Optional.ofNullable(
        edsClient.getByTypeAndIdentifyingAttributes(tenantIdFQNPair.getLeft(), request));
  }
}
