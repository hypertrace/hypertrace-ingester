package org.hypertrace.traceenricher.enrichment.enrichers.endpoint;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.constants.v1.ApiAttribute;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraphBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enriches the Entry Span with API attributes. The API attributes are based on operation_name tag
 * on a Span.
 */
public class EndpointEnricher extends AbstractTraceEnricher {

  private static final Logger LOGGER = LoggerFactory.getLogger(EndpointEnricher.class);

  private static final String API_NAME_ATTR_NAME =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_NAME);
  private static final String API_ID_ATTR_NAME =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_ID);
  private static final String API_URL_PATTERN_ATTR_NAME =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_URL_PATTERN);
  private static final String API_DISCOVERY_STATE_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_DISCOVERY_STATE);

  private ApiEntityDao apiEntityDao;

  // serviceId -> EndpointDiscoverer
  private final Map<String, OperationNameBasedEndpointDiscoverer> serviceIdToEndpointDiscoverer =
      new ConcurrentHashMap<>();

  @Override
  public void init(Config enricherConfig, ClientRegistry clientRegistry) {
    this.apiEntityDao = new ApiEntityDao(clientRegistry.getEdsCacheClient());
  }

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    // This Enricher depends on SpanType so if that's missing, we can't do anything with the span.
    if (event.getEnrichedAttributes() == null) {
      return;
    }

    Map<String, AttributeValue> attributeMap = event.getEnrichedAttributes().getAttributeMap();
    if (attributeMap == null || attributeMap.isEmpty()) {
      return;
    }

    if (!EnrichedSpanUtils.isEntryApiBoundary(event)) {
      return;
    }

    // Lookup the service id from the span. If we can't find a service id, we can't really
    // associate API details with that span.
    String serviceId = EnrichedSpanUtils.getServiceId(event);
    String customerId = trace.getCustomerId();
    if (serviceId == null) {
      LOGGER.warn(
          "Could not find serviceId in the span so not enriching it with API."
              + "tenantId: {}, traceId: {}, span: {}",
          event.getCustomerId(),
          HexUtils.getHex(trace.getTraceId()),
          event);
      return;
    }

    Entity apiEntity = null;
    try {
      apiEntity =
          getOperationNameBasedEndpointDiscoverer(customerId, serviceId).getApiEntity(event);
    } catch (Exception e) {
      LOGGER.error(
          "Unable to get apiEntity for tenantId {}, serviceId {} and event {}",
          customerId,
          serviceId,
          event,
          e);
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "tenantId: {}, serviceId: {}, span: {}, apiEntity: {}",
          customerId,
          serviceId,
          event,
          apiEntity);
    }

    if (apiEntity != null) {
      // API Id
      addEnrichedAttribute(
          event, API_ID_ATTR_NAME, AttributeValueCreator.create(apiEntity.getEntityId()));

      // API pattern
      addEnrichedAttribute(
          event,
          API_URL_PATTERN_ATTR_NAME,
          AttributeValueCreator.create(apiEntity.getEntityName()));

      // API name
      org.hypertrace.entity.data.service.v1.AttributeValue apiNameValue =
          apiEntity.getAttributesMap().get(API_NAME_ATTR_NAME);
      if (apiNameValue != null) {
        addEnrichedAttribute(
            event,
            API_NAME_ATTR_NAME,
            AttributeValueCreator.create(apiNameValue.getValue().getString()));
      }

      // API Discovery
      org.hypertrace.entity.data.service.v1.AttributeValue apiDiscoveryState =
          apiEntity.getAttributesMap().get(API_DISCOVERY_STATE_ATTR);
      if (apiDiscoveryState != null) {
        addEnrichedAttribute(
            event,
            API_DISCOVERY_STATE_ATTR,
            AttributeValueCreator.create(apiDiscoveryState.getValue().getString()));
      }
    }
  }

  @VisibleForTesting
  void setApiEntityDao(ApiEntityDao apiEntityDao) {
    this.apiEntityDao = apiEntityDao;
  }

  private OperationNameBasedEndpointDiscoverer getOperationNameBasedEndpointDiscoverer(
      String customerId, String serviceId) {
    serviceIdToEndpointDiscoverer.computeIfAbsent(
        serviceId,
        e -> new OperationNameBasedEndpointDiscoverer(customerId, serviceId, apiEntityDao));
    return serviceIdToEndpointDiscoverer.get(serviceId);
  }

  /**
   * All the spans within the same API Trace graph gets the same API id as their representative span
   * i.e the entry boundary span
   */
  @Override
  public void enrichTrace(StructuredTrace trace) {
    List<ApiNode<Event>> apiNodes = ApiTraceGraphBuilder.buildGraph(trace).getApiNodeList();
    for (ApiNode<Event> apiNode : apiNodes) {
      Optional<Event> optionalEvent = apiNode.getEntryApiBoundaryEvent();
      if (optionalEvent.isEmpty()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Skipping enrichment, since api node has no entry api boundary event. ApiNode exit event ids {}",
              apiNode.getExitApiBoundaryEvents().stream()
                  .map(event -> HexUtils.getHex(event.getEventId()))
                  .collect(Collectors.toList()));
        }
        continue;
      }

      Event entryApiBoundaryEvent = optionalEvent.get();

      Optional<String> apiId =
          Optional.ofNullable(EnrichedSpanUtils.getApiId(entryApiBoundaryEvent));
      Optional<String> apiPattern =
          Optional.ofNullable(EnrichedSpanUtils.getApiPattern(entryApiBoundaryEvent));
      Optional<String> apiName =
          Optional.ofNullable(EnrichedSpanUtils.getApiName(entryApiBoundaryEvent));
      if (apiId.isEmpty() || apiPattern.isEmpty() || apiName.isEmpty()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Entry API boundary event should have non null apiId, apiPattern and apiName:"
                  + " traceId: {}, eventId: {}, eventName: {}, serviceName: {}, apiId: {}, apiPattern: {},"
                  + " apiName: {}",
              HexUtils.getHex(trace.getTraceId()),
              HexUtils.getHex(entryApiBoundaryEvent.getEventId()),
              entryApiBoundaryEvent.getEventName(),
              entryApiBoundaryEvent.getServiceName(),
              apiId,
              apiPattern,
              apiName);
        }
      } else {
        List<Event> events = apiNode.getEvents();
        for (Event event : events) {
          addEnrichedAttributeIfNotNull(event, API_ID_ATTR_NAME, apiId.get());
          addEnrichedAttributeIfNotNull(event, API_URL_PATTERN_ATTR_NAME, apiPattern.get());
          addEnrichedAttributeIfNotNull(event, API_NAME_ATTR_NAME, apiName.get());
        }
      }
    }
  }
}
