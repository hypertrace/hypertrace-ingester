package org.hypertrace.core.spannormalizer.client;

import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.spannormalizer.config.ConfigServiceConfig;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesRequest;
import org.hypertrace.span.processing.config.service.v1.GetAllExcludeSpanRulesResponse;
import org.hypertrace.span.processing.config.service.v1.SpanProcessingConfigServiceGrpc;

public class ConfigServiceClient {
  private final SpanProcessingConfigServiceGrpc.SpanProcessingConfigServiceBlockingStub
      configServiceStub;

  public ConfigServiceClient(ConfigServiceConfig config, GrpcChannelRegistry channelRegistry) {
    this.configServiceStub =
        SpanProcessingConfigServiceGrpc.newBlockingStub(
                channelRegistry.forPlaintextAddress(
                    config.getConfigServiceHost(), config.getConfigServicePort()))
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  public GetAllExcludeSpanRulesResponse getAllExcludeSpanRules(RequestContext requestContext) {
    return requestContext.call(
        () ->
            configServiceStub.getAllExcludeSpanRules(
                GetAllExcludeSpanRulesRequest.newBuilder().build()));
  }
}
