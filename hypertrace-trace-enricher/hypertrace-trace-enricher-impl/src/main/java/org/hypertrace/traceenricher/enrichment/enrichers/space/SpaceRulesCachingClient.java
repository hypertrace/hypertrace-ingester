package org.hypertrace.traceenricher.enrichment.enrichers.space;

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
import org.hypertrace.spaces.config.service.v1.GetRulesRequest;
import org.hypertrace.spaces.config.service.v1.SpaceConfigRule;
import org.hypertrace.spaces.config.service.v1.SpacesConfigServiceGrpc;
import org.hypertrace.spaces.config.service.v1.SpacesConfigServiceGrpc.SpacesConfigServiceBlockingStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SpaceRulesCachingClient {
  private static final Logger LOG = LoggerFactory.getLogger(SpaceRulesCachingClient.class);

  private final SpacesConfigServiceBlockingStub configServiceStub;

  public SpaceRulesCachingClient(Channel spacesConfigChannel) {
    this.configServiceStub =
        SpacesConfigServiceGrpc.newBlockingStub(spacesConfigChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  private final LoadingCache<String, List<SpaceConfigRule>> cache =
      CacheBuilder.newBuilder()
          .expireAfterWrite(3, TimeUnit.MINUTES)
          .maximumWeight(10_000)
          .weigher((Weigher<String, List<SpaceConfigRule>>) (key, value) -> value.size())
          .build(CacheLoader.from(this::loadRulesForTenant));

  public List<SpaceConfigRule> getRulesForTenant(String tenantId) {
    try {
      return cache.get(tenantId);
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
