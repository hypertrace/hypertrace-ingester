package org.hypertrace.traceenricher.enrichment.clients;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeServiceGrpc.AttributeServiceBlockingStub;
import org.hypertrace.core.attribute.service.v1.GetAttributesRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.trace.provider.AttributeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class AttributeServiceCachedClient implements AttributeProvider {
  private static final RateLimiter LOG_LIMITER = RateLimiter.create(1/60d);
  private static final Logger LOGGER = LoggerFactory.getLogger(AttributeServiceCachedClient.class);
  private final LoadingCache<String, Table<String, String, AttributeMetadata>> cache;
  private final Cache<String, AttributeScopeAndKey> scopeAndKeyLookup;
  private final AttributeServiceBlockingStub attributeServiceBlockingStub;
  private final long deadlineMs;
  AttributeServiceCachedClient(AttributeServiceBlockingStub attributeServiceBlockingStub, int cacheMaxSize, Duration expireAfterWriteDuration, Duration refreshAfterWriteDuration, Duration deadline, int executorThreads) {
    deadlineMs = deadline.toMillis();
    this.attributeServiceBlockingStub = attributeServiceBlockingStub;
    cache = CacheBuilder.newBuilder().maximumSize(cacheMaxSize).expireAfterWrite(expireAfterWriteDuration).refreshAfterWrite(refreshAfterWriteDuration).build(CacheLoader.asyncReloading(
      CacheLoader.from(this::loadTable),
      Executors.newFixedThreadPool(executorThreads, this.buildThreadFactory())));
    scopeAndKeyLookup = CacheBuilder.newBuilder().expireAfterWrite(expireAfterWriteDuration).build();
  }

  public Optional<AttributeMetadata> get(String tenantId, String attributeScope, String attributeKey) {
    try {
      return Optional.ofNullable(cache.getUnchecked(tenantId)).map(table -> table.get(attributeScope, attributeKey));
    } catch (Exception e) {
      if (LOG_LIMITER.tryAcquire()) {
        LOGGER.error("No attribute available for scope {} and key {}", attributeScope, attributeKey);
      }
      return Optional.empty();
    }
  }

  public Optional<AttributeMetadata> getById(String tenantId, String attributeId) {
    try {
      return Optional.ofNullable(cache.getUnchecked(tenantId)).flatMap(table -> Optional.ofNullable(scopeAndKeyLookup.getIfPresent(attributeId)).map(scopeAndKey -> table.get(scopeAndKey.scope, scopeAndKey.key)));
    } catch (Exception e) {
      if (LOG_LIMITER.tryAcquire()) {
        LOGGER.error("No attribute available for id {}", attributeId);
      }
      return Optional.empty();
    }
  }

  public Optional<List<AttributeMetadata>> getAllInScope(String tenantId, String attributeScope) {
    try {
      return Optional.ofNullable(cache.getUnchecked(tenantId)).map(table -> List.copyOf(table.row(attributeScope).values()));
    } catch (Exception e) {
      if (LOG_LIMITER.tryAcquire()) {
        LOGGER.error("No attributes available for scope {}", attributeScope);
      }
      return Optional.empty();
    }
  }

  private Table<String,String,AttributeMetadata> loadTable(String tenantId) {
    List<AttributeMetadata> attributeMetadataList = RequestContext.forTenantId(tenantId).call(() -> attributeServiceBlockingStub.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS).getAttributes(GetAttributesRequest.getDefaultInstance())).getAttributesList();
    attributeMetadataList.forEach(attributeMetadata -> scopeAndKeyLookup.put(attributeMetadata.getId(), new AttributeScopeAndKey(attributeMetadata.getScopeString(), attributeMetadata.getKey())));
    return attributeMetadataList.stream().collect(ImmutableTable.toImmutableTable(AttributeMetadata::getScopeString, AttributeMetadata::getKey, Function.identity()));
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
