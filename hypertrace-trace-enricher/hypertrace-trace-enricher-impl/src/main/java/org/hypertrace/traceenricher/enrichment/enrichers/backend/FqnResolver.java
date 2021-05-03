package org.hypertrace.traceenricher.enrichment.enrichers.backend;

import org.hypertrace.core.datamodel.Event;

// Resolves a FQN (service, backend) from host name and event properties
public interface FqnResolver {
  String resolve(String hostname, Event event);
}
