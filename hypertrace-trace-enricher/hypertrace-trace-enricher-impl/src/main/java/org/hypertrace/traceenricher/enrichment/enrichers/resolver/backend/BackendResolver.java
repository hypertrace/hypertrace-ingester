package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.List;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;

/**
 * Composite backend entity resolver which tries to resolve the backend entity using other resolvers
 */
public class BackendResolver extends AbstractBackendResolver {
  private final List<AbstractBackendResolver> backendResolvers;

  public BackendResolver() {
    backendResolvers =
        List.of(
            new HttpBackendResolver(),
            new GrpcBackendResolver(),
            new RedisBackendResolver(),
            new MongoBackendResolver(),
            new JdbcBackendResolver(),
            new RabbitMqBackendResolver(),
            new KafkaBackendResolver(),
            new SqsBackendResolver(),
            new ClientSpanEndpointResolver());
  }

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    for (AbstractBackendResolver backendResolver : backendResolvers) {
      Optional<BackendInfo> backendInfo = backendResolver.resolve(event, structuredTraceGraph);
      if (backendInfo.isPresent()) {
        return backendInfo;
      }
    }
    return Optional.empty();
  }
}
