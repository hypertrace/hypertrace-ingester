package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import com.google.common.base.Suppliers;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.semantic.convention.utils.span.SpanSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientSpanEndpointProvider implements BackendProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientSpanEndpointProvider.class);

  private Supplier<String> serviceNameSupplier;

  @Override
  public void init(Event event) {
    this.serviceNameSupplier = Suppliers.memoize(event::getServiceName)::get;
  }

  @Override
  public boolean isValidBackend(Event event) {
    return getServiceName() != null;
  }

  @Override
  public BackendType getBackendType(Event event) {
    return BackendType.UNKNOWN;
  }

  /**
   * Checks if the exit span's jaeger_svcname is different from its parent span's jaeger_svcname and
   * proceeds to register a backend for the exit span's jaeger_svcname
   *
   * <p>If the service names are same currently that case isn't handled as we need a way to figure
   * out the destination endpoint
   */
  @Override
  public Optional<String> getBackendUri(Event event, StructuredTraceGraph structuredTraceGraph) {
    String serviceName = getServiceName();
    if (serviceName == null) {
      return Optional.empty();
    }

    Event parentSpan = structuredTraceGraph.getParentEvent(event);
    if (parentSpan != null) {
      String parentSpanServiceName = EnrichedSpanUtils.getServiceName(parentSpan);
      if (!serviceName.equals(parentSpanServiceName)) {
        LOGGER.debug(
            "Detected exit span whose service name is different from its parent span service name. "
                + "Will be creating a backend based on exit span service name = [{}]",
            serviceName);
        return Optional.of(serviceName);
      }
    } else {
      // if its broken span, create backend entity using peer.service
      String peerServiceName = SpanSemanticConventionUtils.getPeerServiceName(event);
      if (peerServiceName != null) {
        LOGGER.debug(
            "Detected exit span which has same peer.service. "
                + "Will be creating a backend based on exit span peer service name = [{}]",
            peerServiceName);
        return Optional.of(peerServiceName);
      }
    }

    return Optional.empty();
  }

  @Override
  public Map<String, AttributeValue> getEntityAttributes(Event event) {
    return Collections.emptyMap();
  }

  @Override
  public Optional<String> getBackendOperation(Event event) {
    return Optional.empty();
  }

  @Override
  public Optional<String> getBackendDestination(Event event) {
    return Optional.empty();
  }

  private String getServiceName() {
    return this.serviceNameSupplier.get();
  }
}
