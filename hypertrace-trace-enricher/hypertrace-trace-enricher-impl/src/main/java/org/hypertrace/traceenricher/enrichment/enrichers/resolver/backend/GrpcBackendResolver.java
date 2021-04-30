package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;

public class GrpcBackendResolver extends AbstractGrpcBackendResolver {

  public GrpcBackendResolver(FqnResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public Optional<String> getBackendUri(Event event) {
    return RpcSemanticConventionUtils.getGrpcURI(event);
  }
}
