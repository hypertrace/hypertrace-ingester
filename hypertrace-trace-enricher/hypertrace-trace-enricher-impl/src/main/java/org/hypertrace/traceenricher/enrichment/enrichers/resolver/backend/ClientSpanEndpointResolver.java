package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.Collections;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.semantic.convention.utils.span.SpanSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientSpanEndpointResolver extends AbstractBackendResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientSpanEndpointResolver.class);

  /**
   * Checks if the exit span's jaeger_svcname is different from its parent span's jaeger_svcname and
   * proceeds to register a backend for the exit span's jaeger_svcname
   *
   * <p>If the service names are same currently that case isn't handled as we need a way to figure
   * out the destination endpoint
   */
  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    String serviceName = event.getServiceName();
    if (serviceName != null) {
      Event parentSpan = structuredTraceGraph.getParentEvent(event);
      if (parentSpan != null) {
        String parentSpanServiceName = EnrichedSpanUtils.getServiceName(parentSpan);
        if (!serviceName.equals(parentSpanServiceName)) {
          LOGGER.debug(
              "Detected exit span whose service name is different from its parent span service name. Will be creating a backend based on exit span service name = [{}]",
              serviceName);
          final Builder backendBuilder =
              getBackendEntityBuilder(BackendType.UNKNOWN, serviceName, event);
          return Optional.of(new BackendInfo(backendBuilder.build(), Collections.emptyMap()));
        }
      } else {
        // if its broken span, create backend entity using peer.service
        String peerServiceName = SpanSemanticConventionUtils.getPeerServiceName(event);
        if (peerServiceName != null) {
          LOGGER.debug(
              "Detected exit span which has same peer.service. Will be creating a backend based on exit span peer service name = [{}]",
              peerServiceName);
          final Builder backendBuilder =
              getBackendEntityBuilder(BackendType.UNKNOWN, peerServiceName, event);
          return Optional.of(backendBuilder.build());
        }
      }
    }

    return Optional.empty();
  }
}
