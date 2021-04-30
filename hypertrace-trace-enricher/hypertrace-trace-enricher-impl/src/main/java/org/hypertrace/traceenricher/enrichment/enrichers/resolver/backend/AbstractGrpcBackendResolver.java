package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeForFirstExistingKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractGrpcBackendResolver extends AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGrpcBackendResolver.class);

  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);
  private static final String BACKEND_DESTINATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_DESTINATION);

  public AbstractGrpcBackendResolver(FqnResolver fqnResolver) {
    super(fqnResolver);
  }

  public abstract Optional<String> getBackendUri(Event event);

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);

    if (protocol == Protocol.PROTOCOL_GRPC) {
      Optional<String> backendURI = getBackendUri(event);

      if (backendURI.isEmpty() || StringUtils.isEmpty(backendURI.get())) {
        LOGGER.debug("Detected GRPC span, but unable to derive the URI. Span Event: {}", event);
        // todo: Proxy with GRPC backend. Proxy supports only HTTP now, and there's no GRPC
        // method information that we can extract from Proxy.
        return Optional.empty();
      }
      final Builder entityBuilder =
          getBackendEntityBuilder(BackendType.GRPC, backendURI.get(), event);
      setAttributeForFirstExistingKey(
          event, entityBuilder, RpcSemanticConventionUtils.getAttributeKeysForGrpcMethod());

      Map<String, AttributeValue> enrichedAttributes = new HashMap<>();
      Optional<String> grpcOperation = RpcSemanticConventionUtils.getRpcOperation(event);
      grpcOperation.ifPresent(
          operation ->
              enrichedAttributes.put(
                  BACKEND_OPERATION_ATTR, AttributeValueCreator.create(operation)));
      Optional<String> grpcDestination = RpcSemanticConventionUtils.getRpcService(event);
      grpcDestination.ifPresent(
          destination ->
              enrichedAttributes.put(
                  BACKEND_DESTINATION_ATTR, AttributeValueCreator.create(destination)));
      return Optional.of(new BackendInfo(entityBuilder.build(), enrichedAttributes));
    }
    return Optional.empty();
  }
}
