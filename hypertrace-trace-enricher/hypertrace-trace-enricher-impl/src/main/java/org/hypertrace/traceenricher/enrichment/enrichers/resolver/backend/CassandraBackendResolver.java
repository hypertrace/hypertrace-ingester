package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraBackendResolver extends AbstractBackendResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraBackendResolver.class);
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);
  private static final String BACKEND_DESTINATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_DESTINATION);

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (!DbSemanticConventionUtils.isCassandraBackend(event)) {
      return Optional.empty();
    }

    Optional<String> backendURI = DbSemanticConventionUtils.getCassandraURI(event);

    if (backendURI.isEmpty() || StringUtils.isEmpty(backendURI.get())) {
      LOGGER.warn("Unable to infer a cassandra backend from event: {}", event);
      return Optional.empty();
    }

    final Builder entityBuilder =
        getBackendEntityBuilder(BackendType.CASSANDRA, backendURI.get(), event);

    Map<String, AttributeValue> enrichedAttributes = new HashMap<>();
    Optional<String> cassandraOperation = DbSemanticConventionUtils.getOtelDbOperation(event);
    cassandraOperation.ifPresent(
        operation ->
            enrichedAttributes.put(
                BACKEND_OPERATION_ATTR, AttributeValueCreator.create(operation)));
    Optional<String> cassandraDestination =
        DbSemanticConventionUtils.getDestinationForCassandra(event);
    cassandraDestination.ifPresent(
        destination ->
            enrichedAttributes.put(
                BACKEND_DESTINATION_ATTR, AttributeValueCreator.create(destination)));
    return Optional.of(new BackendInfo(entityBuilder.build(), enrichedAttributes));
  }
}
