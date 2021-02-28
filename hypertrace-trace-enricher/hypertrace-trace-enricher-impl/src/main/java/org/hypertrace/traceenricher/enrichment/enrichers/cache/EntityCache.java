package org.hypertrace.traceenricher.enrichment.enrichers.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;

/**
 * Class that holds all the entity related caches used by the enrichers
 */
public class EntityCache {
  private final EdsClient edsClient;

  /**
   * Cache to cache the service fqn to service Entity mapping so that we don't
   * look it up over and over.
   */
  private final LoadingCache<Pair<String, String>, Optional<Entity>>
      fqnToServiceEntity =
      CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES)
          .build(new CacheLoader<>() {
            public Optional<Entity> load(@Nonnull Pair<String, String> pair) {
              AttributeValue fqnAttribute = AttributeValue.newBuilder()
                  .setValue(Value.newBuilder().setString(pair.getRight())).build();

              ByTypeAndIdentifyingAttributes request =
                  ByTypeAndIdentifyingAttributes.newBuilder()
                      .setEntityType(EntityType.SERVICE.name())
                      .putIdentifyingAttributes(
                          EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN), fqnAttribute)
                      .build();

              return Optional.ofNullable(edsClient.getByTypeAndIdentifyingAttributes(pair.getLeft(),
                  request));
            }
          });

  /**
   * Cache to cache the service name to a list of services mapping so that we don't look it up
   * over and over.
   */
  private final LoadingCache<Pair<String, String>, List<Entity>> nameToServiceEntities =
      CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES)
          .build(new CacheLoader<>() {
            @Override
            public List<Entity> load(@Nonnull Pair<String, String> pair) {
              // Lookup by name first, to see if there are any services with that name.
              return edsClient.getEntitiesByName(pair.getLeft(), EntityType.SERVICE.name(), pair.getRight());
            }
          });

  /**
   * Cache of K8S namespaces
   * Key: Customer Id, Namespace name
   * Value: List of Namespace entity ids
   */
  private final LoadingCache<Pair<String, String>, List<Entity>> namespaceCache =
      CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES)
          .build(new CacheLoader<>() {
            @Override
            public List<Entity> load(@Nonnull Pair<String, String> key) {
              return edsClient.getEntitiesByName(
                  key.getLeft(), EntityType.K8S_NAMESPACE.name(), key.getRight());
            }
          });

  /**
   * Cache of Backend identifying attributes to Entity
   * Key: Map of identifying attributes
   * Value: Optional Backend entity
   */
  private final LoadingCache<Pair<String, Map<String, AttributeValue>>, Optional<Entity>> backendIdAttrsToEntityCache =
      CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES)
          .build(new CacheLoader<>() {
            @Override
            public Optional<Entity> load(@Nonnull Pair<String, Map<String, AttributeValue>> pair) {
              ByTypeAndIdentifyingAttributes request =
                  ByTypeAndIdentifyingAttributes.newBuilder()
                      .setEntityType(EntityType.BACKEND.name())
                      .putAllIdentifyingAttributes(pair.getRight())
                      .build();
              return Optional.ofNullable(edsClient.getByTypeAndIdentifyingAttributes(pair.getLeft(),
                  request));
            }
          });

  public EntityCache(EdsClient edsClient) {
    this.edsClient = edsClient;
  }

  public LoadingCache<Pair<String, String>, Optional<Entity>> getFqnToServiceEntityCache() {
    return fqnToServiceEntity;
  }

  public LoadingCache<Pair<String, String>, List<Entity>> getNameToServiceEntitiesCache() {
    return nameToServiceEntities;
  }

  public LoadingCache<Pair<String, String>, List<Entity>> getNameToNamespaceEntityIdCache() {
    return namespaceCache;
  }

  public LoadingCache<Pair<String, Map<String, AttributeValue>>, Optional<Entity>> getBackendIdAttrsToEntityCache() {
    return backendIdAttrsToEntityCache;
  }
}
