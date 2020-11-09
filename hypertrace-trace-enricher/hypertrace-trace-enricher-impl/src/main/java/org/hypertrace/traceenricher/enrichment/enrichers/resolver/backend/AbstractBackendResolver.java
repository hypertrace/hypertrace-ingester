package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.createAttributeValue;

import com.google.common.base.Joiner;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;

/**
 * Abstract class with common methods for all the different types of backend resolvers
 */
public abstract class AbstractBackendResolver {
  private static final String COLON = ":";
  private static final Joiner COLON_JOINER = Joiner.on(COLON);
  private static final String BACKEND_PROTOCOL_ATTR_NAME =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL);
  private static final String BACKEND_HOST_ATTR_NAME =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST);
  private static final String BACKEND_PORT_ATTR_NAME =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT);
  private final static String DEFAULT_PORT = "-1";

  private FQNResolver fqnResolver;

  public AbstractBackendResolver(FQNResolver fqnResolver) {
    this.fqnResolver = fqnResolver;
  }

  public abstract Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph);

  Builder getBackendEntityBuilder(BackendType type, String backendURI, Event event) {
    String[] hostAndPort = backendURI.split(COLON);
    String host = hostAndPort[0];
    String port = (hostAndPort.length == 2) ? hostAndPort[1] : DEFAULT_PORT;
    String fqn = fqnResolver.resolve(host, event);
    String entityName = port.equals(DEFAULT_PORT) ? fqn : COLON_JOINER.join(fqn, port);
    final Builder entityBuilder =
        org.hypertrace.entity.data.service.v1.Entity.newBuilder()
            .setEntityType(EntityType.BACKEND.name())
            .setTenantId(event.getCustomerId()).setEntityName(entityName)
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
