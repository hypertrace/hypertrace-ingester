package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.BackendType;

public class CassandraBackendProvider implements BackendProvider {

  @Override
  public void init(Event event) {}

  @Override
  public boolean isValidBackend(Event event) {
    return DbSemanticConventionUtils.isCassandraBackend(event);
  }

  @Override
  public BackendType getBackendType(Event event) {
    return BackendType.CASSANDRA;
  }

  @Override
  public Optional<String> getBackendUri(Event event, StructuredTraceGraph structuredTraceGraph) {
    return DbSemanticConventionUtils.getCassandraURI(event);
  }

  @Override
  public Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> getEntityAttributes(
      Event event) {
    return Collections.emptyMap();
  }

  @Override
  public Optional<String> getBackendOperation(Event event) {
    return DbSemanticConventionUtils.getDbOperation(event);
  }

  @Override
  public Optional<String> getBackendDestination(Event event) {
    return DbSemanticConventionUtils.getDestinationForCassandra(event);
  }
}
