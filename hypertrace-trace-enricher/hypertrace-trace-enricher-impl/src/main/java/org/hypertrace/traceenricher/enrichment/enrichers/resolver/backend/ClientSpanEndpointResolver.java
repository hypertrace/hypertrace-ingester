package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.JaegerAttribute;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientSpanEndpointResolver extends AbstractBackendResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientSpanEndpointResolver.class);

  public ClientSpanEndpointResolver(
      FQNResolver fqnResolver) {
    super(fqnResolver);
  }

  /**
   * Checks if the exit span's jaeger_svcname is different from its parent span's jaeger_svcname and
   * proceeds to register a backend for the exit span's jaeger_svcname
   * <p>
   * If the service names are same currently that case isn't handled as we need a way to figure out
   * the destination endpoint
   */
  @Override
  public Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph) {
    String serviceName = event.getServiceName();
    if (serviceName != null) {
      Event parentSpan = structuredTraceGraph.getParentEvent(event);
      if (parentSpan != null) {
        String parentSpanServiceName = EnrichedSpanUtils.getServiceName(parentSpan);
        if (!serviceName.equals(parentSpanServiceName)) {
          LOGGER.debug(
              "Detected exit span whose service name is different from its parent span service name. Will be creating a backend based on exit span service name = [{}]",
              serviceName);
          final Builder backendBuilder = getBackendEntityBuilder(BackendType.UNKNOWN, serviceName,
              event);
          return Optional.of(backendBuilder.build());
        }
      }
    }

    return Optional.empty();
  }
}
