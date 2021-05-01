package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.semantic.convention.utils.messaging.MessagingSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;

public class KafkaBackendResolver extends AbstractBackendResolver {
  public KafkaBackendResolver(FqnResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public boolean isValidBackend(Event event) {
    return MessagingSemanticConventionUtils.isKafkaBackend(event);
  }

  @Override
  public BackendType getBackendType(Event event) {
    return BackendType.KAFKA;
  }

  @Override
  public Optional<String> getBackendUri(Event event, StructuredTraceGraph structuredTraceGraph) {
    return MessagingSemanticConventionUtils.getKafkaBackendURI(event);
  }

  @Override
  public Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> getEntityAttributes(
      Event event) {
    return Collections.emptyMap();
  }

  @Override
  public Optional<String> getBackendOperation(Event event) {
    return MessagingSemanticConventionUtils.getMessagingOperation(event);
  }

  @Override
  public Optional<String> getBackendDestination(Event event) {
    return MessagingSemanticConventionUtils.getMessagingDestinationForKafka(event);
  }
}
