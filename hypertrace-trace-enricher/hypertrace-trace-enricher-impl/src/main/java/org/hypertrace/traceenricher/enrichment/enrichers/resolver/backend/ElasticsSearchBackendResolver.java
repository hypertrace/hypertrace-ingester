package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeIfExist;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.semantic.convention.constants.db.OTelDbSemanticConventions;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsSearchBackendResolver extends AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsSearchBackendResolver.class);
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);
  private static final String BACKEND_DESTINATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_DESTINATION);

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (!DbSemanticConventionUtils.isElasticSearchBackend(event)) {
      return Optional.empty();
    }

    Optional<String> backendURI = DbSemanticConventionUtils.getElasticSearchURI(event);

    if (backendURI.isEmpty() || StringUtils.isEmpty(backendURI.get())) {
      LOGGER.warn("Unable to infer a elasticsearch backend from event: {}", event);
      return Optional.empty();
    }

    final Builder entityBuilder =
        getBackendEntityBuilder(BackendType.ELASTICSEARCH, backendURI.get(), event);
    setAttributeIfExist(event, entityBuilder, OTelDbSemanticConventions.DB_OPERATION.getValue());

    Map<String, AttributeValue> enrichedAttributes = new HashMap<>();
    Optional<String> esOperation = DbSemanticConventionUtils.getDbOperationForElasticSearch(event);
    esOperation.ifPresent(
        operation ->
            enrichedAttributes.put(
                BACKEND_OPERATION_ATTR, AttributeValueCreator.create(operation)));
    Optional<String> esDestination =
        DbSemanticConventionUtils.getDestinationForElasticsearch(event);
    esDestination.ifPresent(
        destination ->
            enrichedAttributes.put(
                BACKEND_DESTINATION_ATTR, AttributeValueCreator.create(destination)));
    return Optional.of(new BackendInfo(entityBuilder.build(), enrichedAttributes));
  }
}
