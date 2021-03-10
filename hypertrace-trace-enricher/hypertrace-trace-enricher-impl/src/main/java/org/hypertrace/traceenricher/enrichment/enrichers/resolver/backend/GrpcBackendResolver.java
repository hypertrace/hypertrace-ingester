package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeForFirstExistingKey;

import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcBackendResolver extends AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcBackendResolver.class);
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);

    if (protocol == Protocol.PROTOCOL_GRPC) {
      Optional<String> backendURI = RpcSemanticConventionUtils.getGrpcURI(event);

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

      String attributeKey =
          SpanAttributeUtils.getFirstAvailableStringAttribute(
              event, RpcSemanticConventionUtils.getAttributeKeysForGrpcMethod());
      AttributeValue operation = SpanAttributeUtils.getAttributeValue(event, attributeKey);

      return Optional.of(
          new BackendInfo(entityBuilder.build(), Map.of(BACKEND_OPERATION_ATTR, operation)));
    }
    return Optional.empty();
  }
}
