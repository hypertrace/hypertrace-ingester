package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.semantic.convention.utils.messaging.MessagingSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRabbitMqBackendResolver extends AbstractBackendResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqBackendResolver.class);
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);
  private static final String BACKEND_DESTINATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_DESTINATION);

  public AbstractRabbitMqBackendResolver(FqnResolver fqnResolver) {
    super(fqnResolver);
  }

  public abstract Optional<String> getBackendUri(Event event);

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (!MessagingSemanticConventionUtils.isRabbitMqBackend(event)) {
      return Optional.empty();
    }

    Optional<String> routingKey = getBackendUri(event);

    if (routingKey.isEmpty() || StringUtils.isEmpty(routingKey.get())) {
      LOGGER.warn("Unable to infer a rabbitmq backend from event: {}", event);
      return Optional.empty();
    }

    Builder entityBuilder = getBackendEntityBuilder(BackendType.RABBIT_MQ, routingKey.get(), event);

    Map<String, AttributeValue> enrichedAttributes = new HashMap<>();
    Optional<String> rabbitmqOperation =
        MessagingSemanticConventionUtils.getRabbitmqOperation(event);
    rabbitmqOperation.ifPresent(
        operation ->
            enrichedAttributes.put(
                BACKEND_OPERATION_ATTR, AttributeValueCreator.create(operation)));
    Optional<String> rabbitmqDestination =
        MessagingSemanticConventionUtils.getMessagingDestinationForRabbitmq(event);
    rabbitmqDestination.ifPresent(
        destination ->
            enrichedAttributes.put(
                BACKEND_DESTINATION_ATTR, AttributeValueCreator.create(destination)));

    return Optional.of(new BackendInfo(entityBuilder.build(), enrichedAttributes));
  }
}
