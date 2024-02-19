package org.hypertrace.traceenricher.enrichment.enrichers.space;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import io.grpc.Channel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.spaces.config.service.v1.GetRulesRequest;
import org.hypertrace.spaces.config.service.v1.SpaceConfigRule;
import org.hypertrace.spaces.config.service.v1.SpacesConfigServiceGrpc;
import org.hypertrace.spaces.config.service.v1.SpacesConfigServiceGrpc.SpacesConfigServiceBlockingStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SpaceRulesCachingClient {
  private static final Logger LOG = LoggerFactory.getLogger(SpaceRulesCachingClient.class);
  private static final String DOT = ".";
  private static final int DEFAULT_CACHE_MAX_SIZE = 10000;
  private final SpacesConfigServiceBlockingStub configServiceStub;

  public SpaceRulesCachingClient(Channel spacesConfigChannel) {
    this.configServiceStub =
        SpacesConfigServiceGrpc.newBlockingStub(spacesConfigChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    registerCacheMetrics("spaceRulesCache", spaceRulesCache, DEFAULT_CACHE_MAX_SIZE);
  }

  private void registerCacheMetrics(String cacheNameSuffix, Cache cache, int cacheMaxSize) {
    String cacheName = this.getClass().getName() + DOT + cacheNameSuffix;
    PlatformMetricsRegistry.registerCache(cacheName, cache, Collections.emptyMap());
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        cacheName, cache, Collections.emptyMap(), cacheMaxSize);
  }

  private final LoadingCache<String, List<SpaceConfigRule>> spaceRulesCache =
      CacheBuilder.newBuilder()
          .expireAfterWrite(3, TimeUnit.MINUTES)
          .maximumWeight(DEFAULT_CACHE_MAX_SIZE)
          .weigher((Weigher<String, List<SpaceConfigRule>>) (key, value) -> value.size())
          .recordStats()
          .build(CacheLoader.from(this::loadRulesForTenant));

  public List<SpaceConfigRule> getRulesForTenant(String tenantId) {
    try {
      return spaceRulesCache.get(tenantId);
    } catch (Exception exception) {
      LOG.error("Error fetching space config rules", exception);
      return Collections.emptyList();
    }
  }

  private List<SpaceConfigRule> loadRulesForTenant(String tenantId) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        tenantId,
        () -> this.configServiceStub.getRules(GetRulesRequest.getDefaultInstance()).getRulesList());
  }
}
