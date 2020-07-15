package org.hypertrace.traceenricher.enrichment.enrichers;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.JaegerAttribute;
import org.hypertrace.entity.constants.v1.ServiceAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.client.EntityDataServiceClientProvider;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.servicetype.ServiceType;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.util.EntityAvroConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enricher to add a service entity to the spans based on the service name in the span.
 */
public class DefaultServiceEntityEnricher extends AbstractTraceEnricher {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultServiceEntityEnricher.class);

  private static final String SERVICE_ID_ATTR_NAME =
      EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_ID);
  private static final String SERVICE_NAME_ATTR_NAME =
      EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_NAME);
  private static final String SPAN_ID_KEY = "span_id";
  private static final String TRACE_ID_KEY = "trace_id";

  private ServiceEntityFactory factory;

  @Override
  public void init(Config enricherConfig, EntityDataServiceClientProvider provider) {
    LOG.info("Initialize DefaultServiceEntityEnricher with Config: {}", enricherConfig.toString());
    EdsClient edsClient = provider.createClient(enricherConfig);
    this.factory = new ServiceEntityFactory(edsClient);
  }

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    // Nothing to do if the span already has service id on it
    if (EnrichedSpanUtils.getServiceId(event) != null) {
      return;
    }

    // If there is serviceName present in the span, just go ahead and create a service
    // entity with those details. This is to support BareMetal case.
    String serviceName =
        event.getServiceName();
    if (serviceName != null) {
      // Check if the exit span's jaeger_svcname is different from the parent span's jaeger_svcname
      // If it is then use the parent span's jaeger_svcname as the exit span's jaeger svc name else
      // just use the jaeger svc name as is.
      // This will give us 2 things:
      // 1. No service corresponding to exit span will be registered
      //    (Typically this is an example where a facade service is created for a backend.
      //    See redis and mysql in HotROD app for an example).
      //    The actual service name on the exit span will instead be registered
      //    as a backend by the {@link ClientSpanEndpointResolver}
      // 2. Enrich the exit span with the parent span's service entity.
      //    This will enable creating an edge between the exit span and the backend

      StructuredTraceGraph graph = StructuredTraceGraph.createGraph(trace);
      if (EnrichedSpanUtils.isExitSpan(event) &&
          SpanAttributeUtils.isLeafSpan(graph, event)) {
        String parentSvcName = findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService(event,
            serviceName, graph).orElse(null);
        serviceName = parentSvcName != null ? parentSvcName : serviceName;
      }

      Map<String, String> attributes =
          Map.of(SPAN_ID_KEY, HexUtils.getHex(event.getEventId()), TRACE_ID_KEY,
              HexUtils.getHex(trace.getTraceId()));
      org.hypertrace.entity.data.service.v1.Entity entity =
          factory.getService(event.getCustomerId(), serviceName,
              ServiceType.JAEGER_SERVICE.name(), attributes);
      org.hypertrace.core.datamodel.Entity avroEntity =
          EntityAvroConverter.convertToAvroEntity(entity, false);
      if (avroEntity != null) {
        addEntity(trace, event, avroEntity);

        addEnrichedAttribute(event, SERVICE_ID_ATTR_NAME,
            AttributeValueCreator.create(avroEntity.getEntityId()));
        addEnrichedAttribute(event, SERVICE_NAME_ATTR_NAME,
            AttributeValueCreator.create(avroEntity.getEntityName()));
      }
    }
  }

  /**
   * Iterates through the ancestor hierarchy looking for the first ancestor
   * that is not an exit span and has a different service name than the current exit span
   */
  @VisibleForTesting
  Optional<String> findServiceNameOfFirstAncestorThatIsNotAnExitSpanAndBelongsToADifferentService(Event event, String svcName,
                                                                                                  StructuredTraceGraph graph) {
    Event parent = graph.getParentEvent(event);
    String parentSvcName = parent != null ? EnrichedSpanUtils.getServiceName(parent) : null;
    while ((parent != null && EnrichedSpanUtils.isExitApiBoundary(parent)) || svcName.equals(parentSvcName)) {
      parent = graph.getParentEvent(parent);
      parentSvcName = parent != null ? EnrichedSpanUtils.getServiceName(parent) : null;
    }
    return Optional.ofNullable(parentSvcName);
  }
}
