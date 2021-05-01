package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.createAttributeValue;

import com.google.common.base.Joiner;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract class with common methods for all the different types of backend resolvers */
public abstract class AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBackendResolver.class);

  private static final String COLON = ":";
  private static final Joiner COLON_JOINER = Joiner.on(COLON);
  private static final String DEFAULT_PORT = "-1";

  private static final String BACKEND_PROTOCOL_ATTR_NAME =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL);
  private static final String BACKEND_HOST_ATTR_NAME =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST);
  private static final String BACKEND_PORT_ATTR_NAME =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT);
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);
  private static final String BACKEND_DESTINATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_DESTINATION);

  private final FqnResolver fqnResolver;

  public AbstractBackendResolver(FqnResolver fqnResolver) {
    this.fqnResolver = fqnResolver;
  }

  public void init(Event event) {}

  public abstract boolean isValidBackend(Event event);

  public abstract BackendType getBackendType(Event event);

  public abstract Optional<String> getBackendUri(
      Event event, StructuredTraceGraph structuredTraceGraph);

  public abstract Map<String, org.hypertrace.entity.data.service.v1.AttributeValue>
      getEntityAttributes(Event event);

  public abstract Optional<String> getBackendOperation(Event event);

  public abstract Optional<String> getBackendDestination(Event event);

  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    init(event);

    if (!isValidBackend(event)) {
      return Optional.empty();
    }

    BackendType type = getBackendType(event);
    Optional<String> maybeBackendUri = getBackendUri(event, structuredTraceGraph);
    if (maybeBackendUri.isEmpty() || StringUtils.isEmpty(maybeBackendUri.get())) {
      LOGGER.error(
          "Unable to infer backend uri from event {} for backend type {}",
          HexUtils.getHex(event.getEventId()),
          type);
      return Optional.empty();
    }

    String backendUri = maybeBackendUri.get();
    final Builder entityBuilder = getBackendEntityBuilder(type, backendUri, event);
    getEntityAttributes(event).forEach(entityBuilder::putAttributes);

    Map<String, AttributeValue> enrichedAttributes = new HashMap<>();
    Optional<String> backendOperation = getBackendOperation(event);
    Optional<String> backendDestination = getBackendDestination(event);
    backendOperation.ifPresent(
        operation ->
            enrichedAttributes.put(
                BACKEND_OPERATION_ATTR, AttributeValueCreator.create(operation)));
    backendDestination.ifPresent(
        destination ->
            enrichedAttributes.put(
                BACKEND_DESTINATION_ATTR, AttributeValueCreator.create(destination)));

    return Optional.of(
        new BackendInfo(entityBuilder.build(), Collections.unmodifiableMap(enrichedAttributes)));
  }

  private Builder getBackendEntityBuilder(BackendType type, String backendURI, Event event) {
    String[] hostAndPort = backendURI.split(COLON);
    String host = hostAndPort[0];
    String port = (hostAndPort.length == 2) ? hostAndPort[1] : DEFAULT_PORT;
    String fqn = fqnResolver.resolve(host, event);
    String entityName = port.equals(DEFAULT_PORT) ? host : COLON_JOINER.join(host, port);
    final Builder entityBuilder =
        org.hypertrace.entity.data.service.v1.Entity.newBuilder()
            .setEntityType(EntityType.BACKEND.name())
            .setTenantId(event.getCustomerId())
            .setEntityName(entityName)
            .putIdentifyingAttributes(BACKEND_PROTOCOL_ATTR_NAME, createAttributeValue(type.name()))
            .putIdentifyingAttributes(BACKEND_HOST_ATTR_NAME, createAttributeValue(fqn))
            .putIdentifyingAttributes(BACKEND_PORT_ATTR_NAME, createAttributeValue(port));
    entityBuilder.putAttributes(
        EnrichedSpanConstants.getValue(Backend.BACKEND_FROM_EVENT),
        createAttributeValue(event.getEventName()));
    entityBuilder.putAttributes(
        EnrichedSpanConstants.getValue(Backend.BACKEND_FROM_EVENT_ID),
        createAttributeValue(HexUtils.getHex(event.getEventId())));
    return entityBuilder;
  }
}
