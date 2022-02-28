package org.hypertrace.core.spannormalizer.jaeger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.client.ConfigServiceClient;
import org.hypertrace.core.spannormalizer.config.ConfigServiceConfig;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;

@Slf4j
public class ExcludeSpanRuleCache {
  private static final String MODEL_CACHE_REFRESH_MILLIS = "cache.refreshAfterWriteMillis";
  private static final String MODEL_CACHE_EXPIRY_MILLIS = "cache.expireAfterWriteMillis";
  private static final String CACHE_NAME = "excludeSpanRuleCache";
  private static final int MODEL_CACHE_REFRESH_MILLIS_DEFAULT = 180000;
  private static final int MODEL_CACHE_EXPIRY_MILLIS_DEFAULT = 300000;
  private static ExcludeSpanRuleCache INSTANCE;
  private final LoadingCache<String, List<ExcludeSpanRule>> excludeSpanRuleCache;

  private ExcludeSpanRuleCache(Config config) {
    int modelCacheRefreshMillis =
        config.hasPath(MODEL_CACHE_REFRESH_MILLIS)
            ? config.getInt(MODEL_CACHE_REFRESH_MILLIS)
            : MODEL_CACHE_REFRESH_MILLIS_DEFAULT;
    int modelCacheExpiryMillis =
        config.hasPath(MODEL_CACHE_EXPIRY_MILLIS)
            ? config.getInt(MODEL_CACHE_EXPIRY_MILLIS)
            : MODEL_CACHE_EXPIRY_MILLIS_DEFAULT;

    ConfigServiceConfig configServiceConfig = new ConfigServiceConfig(config);
    ConfigServiceClient configServiceClient =
        new ConfigServiceClient(configServiceConfig, new GrpcChannelRegistry());
    this.excludeSpanRuleCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(modelCacheRefreshMillis, TimeUnit.MILLISECONDS)
            .expireAfterWrite(modelCacheExpiryMillis, TimeUnit.MILLISECONDS)
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    new CacheLoader<>() {
                      @Override
                      public List<ExcludeSpanRule> load(@Nonnull String tenantId) {
                        try {
                          return configServiceClient
                              .getAllExcludeSpanRules(RequestContext.forTenantId(tenantId))
                              .getRulesList();
                        } catch (Exception e) {
                          log.error(
                              "Could not get all exclude span rules for tenant id {}:{}",
                              tenantId,
                              e);
                          return Collections.emptyList();
                        }
                      }
                    },
                    Executors.newSingleThreadExecutor()));
    PlatformMetricsRegistry.registerCache(CACHE_NAME, excludeSpanRuleCache, Collections.emptyMap());
  }

  public static synchronized ExcludeSpanRuleCache getInstance(Config config) {
    if (INSTANCE == null) {
      INSTANCE = new ExcludeSpanRuleCache(config);
    }
    return INSTANCE;
  }

  public List<ExcludeSpanRule> get(String tenantId) throws ExecutionException {
    return excludeSpanRuleCache.get(tenantId);
  }
}
