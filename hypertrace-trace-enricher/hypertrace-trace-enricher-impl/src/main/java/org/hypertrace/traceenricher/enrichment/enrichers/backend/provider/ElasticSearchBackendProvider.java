package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import static org.hypertrace.traceenricher.util.EnricherUtil.getAttributesForFirstExistingKey;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.semantic.convention.constants.db.OTelDbSemanticConventions;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.BackendType;

public class ElasticSearchBackendProvider implements BackendProvider {
  @Override
  public void init(Event event) {}

  @Override
  public boolean isValidBackend(Event event) {
    return DbSemanticConventionUtils.isElasticSearchBackend(event);
  }

  @Override
  public BackendType getBackendType(Event event) {
    return BackendType.ELASTICSEARCH;
  }

  @Override
  public Optional<String> getBackendUri(Event event, StructuredTraceGraph structuredTraceGraph) {
    return DbSemanticConventionUtils.getElasticSearchURI(event);
  }

  @Override
  public Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> getEntityAttributes(
      Event event) {
    return getAttributesForFirstExistingKey(
        event, List.of(OTelDbSemanticConventions.DB_OPERATION.getValue()));
  }

  @Override
  public Optional<String> getBackendOperation(Event event) {
    return DbSemanticConventionUtils.getDbOperationForElasticSearch(event);
  }

  @Override
  public Optional<String> getBackendDestination(Event event) {
    return DbSemanticConventionUtils.getDestinationForElasticsearch(event);
  }
}
