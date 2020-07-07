package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.RabbitMq;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMqBackendResolver extends AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqBackendResolver.class);
  private static final String RABBITMQ_ROUTING_KEY_ATTR =
      RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY);

  public RabbitMqBackendResolver(FQNResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (SpanAttributeUtils.containsAttributeKey(event, RABBITMQ_ROUTING_KEY_ATTR)) {
      String routingKey =
          SpanAttributeUtils.getStringAttribute(event, RABBITMQ_ROUTING_KEY_ATTR);
      if (StringUtils.isEmpty(routingKey)) {
        LOGGER.warn("Unable to infer a rabbitmq backend from event: {}", event);
        return Optional.empty();
      }
      final Builder entityBuilder = getBackendEntityBuilder(BackendType.RABBIT_MQ, routingKey,
          event);
      return Optional.of(entityBuilder.build());
    }
    return Optional.empty();
  }
}
