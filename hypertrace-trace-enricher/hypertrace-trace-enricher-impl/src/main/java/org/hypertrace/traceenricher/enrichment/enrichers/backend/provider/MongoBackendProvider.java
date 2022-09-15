package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import static org.hypertrace.traceenricher.util.EnricherUtil.getAttributesForFirstExistingKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.BackendType;

public class MongoBackendProvider implements BackendProvider {
  private static final String RAW_MONGO_NAMESPACE = "NAMESPACE";

  @Override
  public void init(Event event) {}

  @Override
  public boolean isValidBackend(Event event) {
    return DbSemanticConventionUtils.isMongoBackend(event);
  }

  @Override
  public BackendType getBackendType(Event event) {
    return BackendType.MONGO;
  }

  @Override
  public Optional<String> getBackendUri(Event event, StructuredTraceGraph structuredTraceGraph) {
    return DbSemanticConventionUtils.getMongoURI(event);
  }

  @Override
  public Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> getEntityAttributes(
      Event event) {
    Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> entityAttributes =
        new HashMap<>();
    entityAttributes.putAll(getAttributesForFirstExistingKey(event, List.of(RAW_MONGO_NAMESPACE)));
    entityAttributes.putAll(
        getAttributesForFirstExistingKey(
            event, DbSemanticConventionUtils.getAttributeKeysForMongoNamespace()));
    entityAttributes.putAll(
        getAttributesForFirstExistingKey(
            event, DbSemanticConventionUtils.getAttributeKeysForMongoOperation()));

    return Collections.unmodifiableMap(entityAttributes);
  }

  @Override
  public Optional<String> getBackendOperation(Event event) {
    return DbSemanticConventionUtils.getDbOperationForMongo(event);
  }

  @Override
  public Optional<String> getBackendDestination(Event event) {
    return DbSemanticConventionUtils.getDestinationForMongo(event);
  }
}
