package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeForFirstExistingKey;

import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.semantic.convention.utils.messaging.MessagingSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMqBackendResolver extends AbstractBackendResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqBackendResolver.class);
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (!MessagingSemanticConventionUtils.isRabbitMqBackend(event)) {
      return Optional.empty();
    }
    Optional<String> routingKey = MessagingSemanticConventionUtils.getRabbitMqRoutingKey(event);

    if (routingKey.isEmpty() || StringUtils.isEmpty(routingKey.get())) {
      LOGGER.warn("Unable to infer a rabbitmq backend from event: {}", event);
      return Optional.empty();
    }

    Builder entityBuilder = getBackendEntityBuilder(BackendType.RABBIT_MQ, routingKey.get(), event);

    setAttributeForFirstExistingKey(
        event,
        entityBuilder,
        MessagingSemanticConventionUtils.getAttributeKeysForMessagingOperation());

    String attributeKey =
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, MessagingSemanticConventionUtils.getAttributeKeysForMessagingOperation());
    AttributeValue operation = SpanAttributeUtils.getAttributeValue(event, attributeKey);

    return Optional.of(
        new BackendInfo(entityBuilder.build(), Map.of(BACKEND_OPERATION_ATTR, operation)));
  }
}
