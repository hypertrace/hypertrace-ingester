package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.semantic.convention.utils.messaging.MessagingSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;

public class RabbitMqBackendResolver extends AbstractBackendResolver {

  public RabbitMqBackendResolver(FqnResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public boolean isValidBackend(Event event) {
    return MessagingSemanticConventionUtils.isRabbitMqBackend(event);
  }

  @Override
  public BackendType getBackendType(Event event) {
    return BackendType.RABBIT_MQ;
  }

  @Override
  public Optional<String> getBackendUri(Event event, StructuredTraceGraph structuredTraceGraph) {
    return MessagingSemanticConventionUtils.getRabbitMqRoutingKey(event);
  }

  @Override
  public Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> getEntityAttributes(
      Event event) {
    return Collections.emptyMap();
  }

  @Override
  public Optional<String> getBackendOperation(Event event) {
    return MessagingSemanticConventionUtils.getRabbitmqOperation(event);
  }

  @Override
  public Optional<String> getBackendDestination(Event event) {
    return MessagingSemanticConventionUtils.getMessagingDestinationForRabbitmq(event);
  }
}
