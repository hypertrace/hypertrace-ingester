package org.hypertrace.core.spannormalizer.jaeger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.client.ConfigServiceClient;
import org.hypertrace.core.spannormalizer.config.ConfigServiceConfig;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleDetails;

@Slf4j
public class ExcludeSpanRulesCache {
  private static final String CACHE_REFRESH_DURATION =
      "span.rules.exclude.cache.refreshAfterWriteDuration";
  private static final String CACHE_EXPIRY_DURATION =
      "span.rules.exclude.cache.expireAfterWriteDuration";
  private static final String CACHE_NAME = "excludeSpanRuleCache";
  private static final Duration CACHE_REFRESH_DURATION_DEFAULT = Duration.ofMillis(180000);
  private static final Duration CACHE_EXPIRY_DURATION_DEFAULT = Duration.ofMillis(300000);
  private static ExcludeSpanRulesCache INSTANCE;
  private final LoadingCache<String, List<ExcludeSpanRule>> excludeSpanRulesCache;

  private ExcludeSpanRulesCache(Config config) {
    Duration cacheRefreshDuration =
        config.hasPath(CACHE_REFRESH_DURATION)
            ? config.getDuration(CACHE_REFRESH_DURATION)
            : CACHE_REFRESH_DURATION_DEFAULT;
    Duration cacheExpiryDuration =
        config.hasPath(CACHE_EXPIRY_DURATION)
            ? config.getDuration(CACHE_EXPIRY_DURATION)
            : CACHE_EXPIRY_DURATION_DEFAULT;

    ConfigServiceConfig configServiceConfig = new ConfigServiceConfig(config);
    ConfigServiceClient configServiceClient =
        new ConfigServiceClient(configServiceConfig, new GrpcChannelRegistry());
    this.excludeSpanRulesCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheRefreshDuration.toMillis(), TimeUnit.MILLISECONDS)
            .expireAfterWrite(cacheExpiryDuration.toMillis(), TimeUnit.MILLISECONDS)
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    new CacheLoader<>() {
                      @Override
                      public List<ExcludeSpanRule> load(@Nonnull String tenantId) {
                        try {
                          return configServiceClient
                              .getAllExcludeSpanRules(RequestContext.forTenantId(tenantId))
                              .getRuleDetailsList()
                              .stream()
                              .map(ExcludeSpanRuleDetails::getRule)
                              .collect(Collectors.toUnmodifiableList());
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
    PlatformMetricsRegistry.registerCache(
        CACHE_NAME, excludeSpanRulesCache, Collections.emptyMap());
  }

  public static synchronized ExcludeSpanRulesCache getInstance(Config config) {
    if (INSTANCE == null) {
      INSTANCE = new ExcludeSpanRulesCache(config);
    }
    return INSTANCE;
  }

  public List<ExcludeSpanRule> get(String tenantId) throws ExecutionException {
    return excludeSpanRulesCache.get(tenantId);
  }
}
