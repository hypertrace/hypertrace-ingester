package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;

public interface BackendProvider {
  void init(Event event);

  boolean isValidBackend(Event event);

  BackendType getBackendType(Event event);

  Optional<String> getBackendUri(Event event, StructuredTraceGraph structuredTraceGraph);

  Map<String, AttributeValue> getEntityAttributes(Event event);

  Optional<String> getBackendOperation(Event event);

  Optional<String> getBackendDestination(Event event);
}
