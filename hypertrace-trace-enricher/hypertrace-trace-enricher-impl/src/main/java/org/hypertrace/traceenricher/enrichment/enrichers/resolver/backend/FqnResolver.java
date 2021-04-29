package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import org.hypertrace.core.datamodel.Event;

public interface FqnResolver {
  String resolve(String hostname, Event event);
}
