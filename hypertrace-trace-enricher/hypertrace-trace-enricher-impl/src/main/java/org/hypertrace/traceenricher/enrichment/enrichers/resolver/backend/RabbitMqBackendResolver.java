package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.semantic.convention.utils.messaging.MessagingSemanticConventionUtils;

public class RabbitMqBackendResolver extends AbstractRabbitMqBackendResolver {

  public RabbitMqBackendResolver(FqnResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public Optional<String> getBackendUri(Event event) {
    return MessagingSemanticConventionUtils.getRabbitMqRoutingKey(event);
  }
}
