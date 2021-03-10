package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeForFirstExistingKey;
import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeIfExist;

import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoBackendResolver extends AbstractBackendResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoBackendResolver.class);
  private static final String RAW_MONGO_NAMESPACE = "NAMESPACE";
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (!DbSemanticConventionUtils.isMongoBackend(event)) {
      return Optional.empty();
    }

    Optional<String> backendURI = DbSemanticConventionUtils.getMongoURI(event);

    if (backendURI.isEmpty() || StringUtils.isEmpty(backendURI.get())) {
      LOGGER.warn("Unable to infer a mongo backend from event: {}", event);
      return Optional.empty();
    }

    final Builder entityBuilder =
        getBackendEntityBuilder(BackendType.MONGO, backendURI.get(), event);
    setAttributeIfExist(event, entityBuilder, RAW_MONGO_NAMESPACE);
    setAttributeForFirstExistingKey(
        event, entityBuilder, DbSemanticConventionUtils.getAttributeKeysForMongoNamespace());
    setAttributeForFirstExistingKey(
        event, entityBuilder, DbSemanticConventionUtils.getAttributeKeysForMongoOperation());

    String attributeKey =
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, DbSemanticConventionUtils.getAttributeKeysForMongoOperation());
    AttributeValue operation = SpanAttributeUtils.getAttributeValue(event, attributeKey);
    return Optional.of(
        new BackendInfo(entityBuilder.build(), Map.of(BACKEND_OPERATION_ATTR, operation)));
  }
}
