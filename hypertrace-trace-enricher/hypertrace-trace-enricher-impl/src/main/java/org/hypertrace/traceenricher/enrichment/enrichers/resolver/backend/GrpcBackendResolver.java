package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeForFirstExistingKey;

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.span.constants.v1.Grpc;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;
import org.hypertrace.traceenricher.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcBackendResolver extends AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcBackendResolver.class);

  public GrpcBackendResolver(FQNResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);

    if (protocol == Protocol.PROTOCOL_GRPC) {
      Optional<String> backendURI = RpcSemanticConventionUtils
          .getGrpcURI(event);

      if (backendURI.isEmpty() || StringUtils.isEmpty(backendURI.get())) {
        LOGGER.debug(
            "Detected GRPC span, but unable to find the {} attribute as the URI. Span Event: {}",
            Constants.getRawSpanConstant(Grpc.GRPC_HOST_PORT), event);
        // todo: Proxy with GRPC backend. Proxy supports only HTTP now, and there's no GRPC
        // method information that we can extract from Proxy.
        return Optional.empty();
      }
      final Builder entityBuilder = getBackendEntityBuilder(BackendType.GRPC, backendURI.get(), event);
      setAttributeForFirstExistingKey(event, entityBuilder, RpcSemanticConventionUtils
          .getAttributeKeysForGrpcMethod());
      return Optional.of(entityBuilder.build());
    }
    return Optional.empty();
  }
}
