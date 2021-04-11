package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.semantic.convention.utils.messaging.MessagingSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaBackendResolver extends AbstractBackendResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaBackendResolver.class);
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);
  private static final String BACKEND_DESTINATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_DESTINATION);

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (!MessagingSemanticConventionUtils.isKafkaBackend(event)) {
      return Optional.empty();
    }

    Optional<String> backendURI = MessagingSemanticConventionUtils.getKafkaBackendURI(event);

    if (backendURI.isEmpty() || StringUtils.isEmpty(backendURI.get())) {
      LOGGER.warn("Unable to infer a kafka backend from event: {}", event);
      return Optional.empty();
    }
    /*
    todo: should we clean protocol constants for rabbit_mq, mongo?
    this is not added into enriched spans constants proto as there want be http.url
    with kafka://bootstrap.9092
    * */
    Entity.Builder entityBuilder =
        getBackendEntityBuilder(BackendType.KAFKA, backendURI.get(), event);

    Map<String, AttributeValue> enrichedAttributes = new HashMap<>();
    Optional<String> kafkaOperation = MessagingSemanticConventionUtils.getMessagingOperation(event);
    kafkaOperation.ifPresent(
        operation ->
            enrichedAttributes.put(
                BACKEND_OPERATION_ATTR, AttributeValueCreator.create(operation)));
    Optional<String> kafkaDestination =
        MessagingSemanticConventionUtils.getMessagingDestinationForKafka(event);
    kafkaDestination.ifPresent(
        destination ->
            enrichedAttributes.put(
                BACKEND_DESTINATION_ATTR, AttributeValueCreator.create(destination)));
    return Optional.of(new BackendInfo(entityBuilder.build(), enrichedAttributes));
  }
}
