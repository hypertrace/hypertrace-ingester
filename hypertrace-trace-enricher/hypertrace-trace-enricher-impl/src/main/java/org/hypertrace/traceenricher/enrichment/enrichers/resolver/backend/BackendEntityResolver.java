package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.List;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;

/**
 * Composite backend entity resolver which tries to resolve the backend entity using other resolvers
 */
public class BackendEntityResolver extends AbstractBackendResolver {
  private final List<AbstractBackendResolver> backendResolvers;

  public BackendEntityResolver(FQNResolver fqnResolver) {
    super(fqnResolver);
    backendResolvers = List.of(
        new HttpBackendResolver(fqnResolver),
        new GrpcBackendResolver(fqnResolver),
        new RedisBackendResolver(fqnResolver),
        new MongoBackendResolver(fqnResolver),
        new JdbcBackendResolver(fqnResolver),
        new RabbitMqBackendResolver(fqnResolver),
        new ClientSpanEndpointResolver(fqnResolver)
    );
  }

  @Override
  public Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph) {
    for (AbstractBackendResolver backendResolver : backendResolvers) {
      Optional<Entity> entity = backendResolver.resolveEntity(event, structuredTraceGraph);
      if (entity.isPresent()) {
        return entity;
      }
    }
    return Optional.empty();
  }
}
