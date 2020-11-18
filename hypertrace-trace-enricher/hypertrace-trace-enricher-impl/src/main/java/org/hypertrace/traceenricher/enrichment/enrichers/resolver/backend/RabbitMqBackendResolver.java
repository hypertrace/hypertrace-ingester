package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.attribute.MessagingSystemTagResolver;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMqBackendResolver extends AbstractBackendResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqBackendResolver.class);

  public RabbitMqBackendResolver(FQNResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph) {
    Optional<String> routingKey = MessagingSystemTagResolver.getRabbitMQRoutingKey(event);
    if (routingKey.isEmpty() || StringUtils.isEmpty(routingKey.get())) {
      LOGGER.warn("Unable to infer a rabbitmq backend from event: {}", event);
      return Optional.empty();
    }

    final Builder entityBuilder = getBackendEntityBuilder(
        BackendType.RABBIT_MQ,
        routingKey.get(), event);
    return Optional.of(entityBuilder.build());
  }
}
