package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.getAttributesForFirstExistingKey;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;

public class GrpcBackendResolver extends AbstractBackendResolver {
  public GrpcBackendResolver(FqnResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public boolean isValidBackend(Event event) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    return protocol == Protocol.PROTOCOL_GRPC;
  }

  @Override
  public BackendType getBackendType(Event event) {
    return BackendType.GRPC;
  }

  @Override
  public Optional<String> getBackendUri(Event event, StructuredTraceGraph structuredTraceGraph) {
    return RpcSemanticConventionUtils.getGrpcURI(event);
  }

  @Override
  public Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> getEntityAttributes(
      Event event) {
    return getAttributesForFirstExistingKey(
        event, RpcSemanticConventionUtils.getAttributeKeysForGrpcMethod());
  }

  @Override
  public Optional<String> getBackendOperation(Event event) {
    return RpcSemanticConventionUtils.getRpcOperation(event);
  }

  @Override
  public Optional<String> getBackendDestination(Event event) {
    return RpcSemanticConventionUtils.getRpcService(event);
  }
}
