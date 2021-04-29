package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import org.hypertrace.core.datamodel.Event;

public class HypertraceFqnResolver implements FqnResolver {
  private static final String SVC_CLUSTER_LOCAL_SUFFIX = ".svc.cluster.local";

  @Override
  public String resolve(String hostname, Event event) {
    return hostname.replace(SVC_CLUSTER_LOCAL_SUFFIX, "");
  }
}
