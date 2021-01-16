package org.hypertrace.traceenricher.enrichment.enrichers.space;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.grpc.Channel;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.spaces.config.service.v1.GetRulesRequest;
import org.hypertrace.spaces.config.service.v1.SpaceConfigRule;
import org.hypertrace.spaces.config.service.v1.SpacesConfigServiceGrpc;
import org.hypertrace.spaces.config.service.v1.SpacesConfigServiceGrpc.SpacesConfigServiceBlockingStub;

class SpaceRulesCachingClient {

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
      return cache.getUnchecked(tenantId);
    } catch (UncheckedExecutionException exception) {
      if (exception.getCause() instanceof RuntimeException) {
        throw (RuntimeException) exception.getCause();
      }
      throw exception;
    }
  }

  private List<SpaceConfigRule> loadRulesForTenant(String tenantId) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        tenantId,
        () -> this.configServiceStub.getRules(GetRulesRequest.getDefaultInstance()).getRulesList());
  }
}
