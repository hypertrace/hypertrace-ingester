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

public class SqsBackendResolver extends AbstractBackendResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqsBackendResolver.class);
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (!MessagingSemanticConventionUtils.isSqsBackend(event)) {
      return Optional.empty();
    }

    Optional<String> backendURI = MessagingSemanticConventionUtils.getSqsBackendURI(event);

    if (backendURI.isEmpty() || StringUtils.isEmpty(backendURI.get())) {
      LOGGER.warn("Unable to infer a SQS backend from event: {}", event);
      return Optional.empty();
    }
    /*
    todo: should we clean protocol constants for rabbit_mq, mongo?
    this is not added into enriched spans constants proto as there want be http.url
    with sqs://xyz:2323.
    * */
    Entity.Builder entityBuilder =
        getBackendEntityBuilder(BackendType.SQS, backendURI.get(), event);

    Map<String, AttributeValue> enrichedAttributes = new HashMap<>();
    String sqsOperation = MessagingSemanticConventionUtils.getMessagingOperation(event);
    if (sqsOperation != null) {
      enrichedAttributes.put(BACKEND_OPERATION_ATTR, AttributeValueCreator.create(sqsOperation));
    }
    return Optional.of(new BackendInfo(entityBuilder.build(), enrichedAttributes));
  }
}
