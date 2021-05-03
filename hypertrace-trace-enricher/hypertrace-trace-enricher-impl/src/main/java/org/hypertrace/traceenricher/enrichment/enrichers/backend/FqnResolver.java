package org.hypertrace.traceenricher.enrichment.enrichers.backend;

import org.hypertrace.core.datamodel.Event;

public interface FqnResolver {
  String resolve(String hostname, Event event);
}
