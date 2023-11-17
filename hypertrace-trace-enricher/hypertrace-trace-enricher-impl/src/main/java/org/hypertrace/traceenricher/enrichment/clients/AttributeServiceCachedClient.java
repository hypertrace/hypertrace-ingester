package org.hypertrace.traceenricher.enrichment.clients;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import io.grpc.Channel;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeServiceGrpc;
import org.hypertrace.core.attribute.service.v1.AttributeServiceGrpc.AttributeServiceBlockingStub;
import org.hypertrace.core.attribute.service.v1.GetAttributesRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.trace.provider.AttributeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AttributeServiceCachedClient implements AttributeProvider {
  private static final String DEADLINE_CONFIG_KEY = "deadline";
  private static final String CACHE_MAX_SIZE_CONFIG_KEY = "maxSize";
  private static final String CACHE_REFRESH_AFTER_WRITE_CONFIG_KEY = "refreshAfterWriteDuration";
  private static final String CACHE_EXPIRE_AFTER_WRITE_CONFIG_KEY = "expireAfterWriteDuration";
  private static final String CACHE_EXECUTOR_THREADS_CONFIG_KEY = "executorThreads";
  private static final RateLimiter LOG_LIMITER = RateLimiter.create(1 / 60d);
  private static final Logger LOGGER = LoggerFactory.getLogger(AttributeServiceCachedClient.class);
  private final LoadingCache<String, Table<String, String, AttributeMetadata>> cache;
  private final Cache<String, AttributeScopeAndKey> scopeAndKeyLookup;
  private final AttributeServiceBlockingStub attributeServiceBlockingStub;
  private final long deadlineMs;

  AttributeServiceCachedClient(Channel channel, Config attributeServiceConfig) {
    deadlineMs =
        attributeServiceConfig.hasPath(DEADLINE_CONFIG_KEY)
            ? attributeServiceConfig.getDuration(DEADLINE_CONFIG_KEY).toMillis()
            : Duration.ofMinutes(1).toMillis();
    this.attributeServiceBlockingStub = AttributeServiceGrpc.newBlockingStub(channel);
    Duration expireAfterWriteDuration =
        attributeServiceConfig.hasPath(CACHE_EXPIRE_AFTER_WRITE_CONFIG_KEY)
            ? attributeServiceConfig.getDuration(CACHE_EXPIRE_AFTER_WRITE_CONFIG_KEY)
            : Duration.ofHours(1);
    cache =
        CacheBuilder.newBuilder()
            .maximumSize(
                attributeServiceConfig.hasPath(CACHE_MAX_SIZE_CONFIG_KEY)
                    ? attributeServiceConfig.getLong(CACHE_MAX_SIZE_CONFIG_KEY)
                    : 100)
            .expireAfterWrite(expireAfterWriteDuration)
            .refreshAfterWrite(
                attributeServiceConfig.hasPath(CACHE_REFRESH_AFTER_WRITE_CONFIG_KEY)
                    ? attributeServiceConfig.getDuration(CACHE_REFRESH_AFTER_WRITE_CONFIG_KEY)
                    : Duration.ofMinutes(15))
            .build(
                CacheLoader.asyncReloading(
                    CacheLoader.from(this::loadTable),
                    Executors.newFixedThreadPool(
                        attributeServiceConfig.hasPath(CACHE_EXECUTOR_THREADS_CONFIG_KEY)
                            ? attributeServiceConfig.getInt(CACHE_EXECUTOR_THREADS_CONFIG_KEY)
                            : 4,
                        this.buildThreadFactory())));
    PlatformMetricsRegistry.registerCache(
        "attribute-service-client-cache", cache, Collections.emptyMap());
    scopeAndKeyLookup =
        CacheBuilder.newBuilder().expireAfterWrite(expireAfterWriteDuration).build();
  }

  public Optional<AttributeMetadata> get(
      String tenantId, String attributeScope, String attributeKey) {
    try {
      return Optional.ofNullable(cache.getUnchecked(tenantId))
          .map(table -> table.get(attributeScope, attributeKey));
    } catch (Exception e) {
      if (LOG_LIMITER.tryAcquire()) {
        LOGGER.error(
            "No attribute available for scope {} and key {}", attributeScope, attributeKey);
      }
      return Optional.empty();
    }
  }

  public Optional<AttributeMetadata> getById(String tenantId, String attributeId) {
    try {
      return Optional.ofNullable(cache.getUnchecked(tenantId))
          .flatMap(
              table ->
                  Optional.ofNullable(scopeAndKeyLookup.getIfPresent(attributeId))
                      .map(scopeAndKey -> table.get(scopeAndKey.scope, scopeAndKey.key)));
    } catch (Exception e) {
      if (LOG_LIMITER.tryAcquire()) {
        LOGGER.error("No attribute available for id {}", attributeId);
      }
      return Optional.empty();
    }
  }

  public Optional<List<AttributeMetadata>> getAllInScope(String tenantId, String attributeScope) {
    try {
      return Optional.ofNullable(cache.getUnchecked(tenantId))
          .map(table -> List.copyOf(table.row(attributeScope).values()));
    } catch (Exception e) {
      if (LOG_LIMITER.tryAcquire()) {
        LOGGER.error("No attributes available for scope {}", attributeScope);
      }
      return Optional.empty();
    }
  }

  private Table<String, String, AttributeMetadata> loadTable(String tenantId) {
    List<AttributeMetadata> attributeMetadataList =
        RequestContext.forTenantId(tenantId)
            .call(
                () ->
                    attributeServiceBlockingStub
                        .withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS)
                        .getAttributes(GetAttributesRequest.getDefaultInstance()))
            .getAttributesList();
    attributeMetadataList.forEach(
        attributeMetadata ->
            scopeAndKeyLookup.put(
                attributeMetadata.getId(),
                new AttributeScopeAndKey(
                    attributeMetadata.getScopeString(), attributeMetadata.getKey())));
    return attributeMetadataList.stream()
        .collect(
            ImmutableTable.toImmutableTable(
                AttributeMetadata::getScopeString, AttributeMetadata::getKey, Function.identity()));
  }

  private ThreadFactory buildThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("attribute-service-cache-%d")
        .build();
  }

  private static final class AttributeScopeAndKey {
    private final String scope;
    private final String key;

    private AttributeScopeAndKey(String scope, String key) {
      this.scope = scope;
      this.key = key;
    }
  }
}
