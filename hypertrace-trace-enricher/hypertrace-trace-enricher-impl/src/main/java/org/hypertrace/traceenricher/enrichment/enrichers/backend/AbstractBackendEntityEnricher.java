package org.hypertrace.traceenricher.enrichment.enrichers.backend;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.semantic.convention.utils.span.SpanSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.BackendType;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.BackendProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.BackendInfo;
import org.hypertrace.traceenricher.util.EnricherUtil;
import org.hypertrace.traceenricher.util.EntityAvroConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enricher which gets the backend entities from trace event and store backend entity into
 * EntityDataService.
 */
public abstract class AbstractBackendEntityEnricher extends AbstractTraceEnricher {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBackendEntityEnricher.class);

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

  private EdsClient edsClient;
  private EntityCache entityCache;
  private FqnResolver fqnResolver;

  @Override
  public void init(Config enricherConfig, ClientRegistry clientRegistry) {
    LOGGER.info("Initialize BackendEntityEnricher with Config: {}", enricherConfig.toString());
    this.edsClient = clientRegistry.getEdsCacheClient();
    this.entityCache = clientRegistry.getEntityCache();
    setup(enricherConfig, clientRegistry);
    this.fqnResolver = getFqnResolver();
  }

  public abstract void setup(Config enricherConfig, ClientRegistry clientRegistry);

  public abstract List<BackendProvider> getBackendProviders();

  public abstract FqnResolver getFqnResolver();

  // At trace level, based on the next span to identify if a backend entity is actually a service
  // entity.
  @Override
  public void enrichTrace(StructuredTrace trace) {
    try {
      StructuredTraceGraph structuredTraceGraph = buildGraph(trace);
      trace.getEventList().stream()
          // filter leaf exit spans only
          .filter(
              event ->
                  EnrichedSpanUtils.isExitSpan(event)
                      && SpanAttributeUtils.isLeafSpan(structuredTraceGraph, event)
                      && canResolveBackend(structuredTraceGraph, event))
          // resolve backend entity
          .map(event -> Pair.of(event, resolve(event, trace, structuredTraceGraph)))
          .filter(pair -> pair.getRight().isPresent())
          // check if backend entity is valid
          .filter(pair -> isValidBackendEntity(trace, pair.getLeft(), pair.getRight().get()))
          // decorate event/trace with backend entity attributes
          .forEach(pair -> decorateWithBackendEntity(pair.getRight().get(), pair.getLeft(), trace));
    } catch (Exception ex) {
      LOGGER.error("An error occurred while enriching backend", ex);
    }
  }

  /**
   * Method to check if backend resolution should proceed. This will enable any custom logic to be
   * inserted in the implementing classes.
   *
   * @param structuredTraceGraph structured trace graph
   * @param event leaf exit span
   * @return true if backend resolution is allowed
   */
  protected boolean canResolveBackend(StructuredTraceGraph structuredTraceGraph, Event event) {
    // by default allow the backend resolution to proceed
    return true;
  }

  /** Checks if the candidateEntity is indeed a backend Entity */
  private boolean isValidBackendEntity(
      StructuredTrace trace, Event backendSpan, BackendInfo candidateInfo) {
    // Always create backend entity for RabbitMq, Mongo, Redis, Jdbc
    String backendProtocol =
        candidateInfo
            .getEntity()
            .getIdentifyingAttributesMap()
            .get(BACKEND_PROTOCOL_ATTR_NAME)
            .getValue()
            .getString();

    BackendType backendType = BackendType.valueOf(backendProtocol);
    if (backendType != BackendType.HTTP
        && backendType != BackendType.HTTPS
        && backendType != BackendType.GRPC) {
      return true;
    }

    // If there is a Service with the same FQN, then it isn't a Backend
    String fqn =
        candidateInfo
            .getEntity()
            .getIdentifyingAttributesMap()
            .get(BACKEND_HOST_ATTR_NAME)
            .getValue()
            .getString();

    if (checkIfServiceEntityExists(trace, backendSpan, fqn, candidateInfo.getEntity())) {
      return false;
    }

    // checks the existence of peer service in case if it is a partial trace, and we are missing
    // its immediate child span.
    String peerServiceName = SpanSemanticConventionUtils.getPeerServiceName(backendSpan);
    if (peerServiceName != null
        && checkIfServiceEntityExists(
            trace, backendSpan, peerServiceName, candidateInfo.getEntity())) {
      return false;
    }

    return true;
  }

  private void decorateWithBackendEntity(
      BackendInfo backendInfo, Event event, StructuredTrace trace) {
    LOGGER.debug(
        "Trying to load or create backend entity: {}, corresponding event: {}",
        backendInfo.getEntity(),
        event);
    Entity backend = createBackendIfMissing(backendInfo.getEntity());
    if (backend == null) {
      LOGGER.warn("Failed to upsert backend entity: {}", backendInfo.getEntity());
      return;
    }
    org.hypertrace.core.datamodel.Entity avroEntity =
        EntityAvroConverter.convertToAvroEntity(backend, true);
    if (avroEntity == null) {
      LOGGER.warn("Error converting backendEntity:{} to avro", backendInfo.getEntity());
      return;
    }

    addEntity(trace, event, avroEntity);
    addEnrichedAttributes(event, getAttributesToEnrich(backend));
    addEnrichedAttributes(event, backendInfo.getAttributes());
  }

  private boolean checkIfServiceEntityExists(
      StructuredTrace trace, Event span, String serviceFqn, Entity candidateBackendEntity) {
    try {
      boolean serviceExists = this.getPossibleService(trace, span, serviceFqn).isPresent();
      if (serviceExists) {
        LOGGER.debug(
            "BackendEntity {} is actually an Existing service entity.", candidateBackendEntity);
      }
      return serviceExists;
    } catch (Exception ex) {
      LOGGER.error(
          "Error getting service entity using FQN:{} from cache for tenantId:{}",
          serviceFqn,
          span.getCustomerId());
      return false;
    }
  }

  private Map<String, org.hypertrace.core.datamodel.AttributeValue> getAttributesToEnrich(
      Entity backend) {
    Map<String, org.hypertrace.core.datamodel.AttributeValue> attributes = new LinkedHashMap<>();

    attributes.put(
        EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_ID),
        AttributeValueCreator.create(backend.getEntityId()));
    attributes.put(
        EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_NAME),
        AttributeValueCreator.create(backend.getEntityName()));
    attributes.put(
        EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST),
        createAttributeValue(
            backend
                .getAttributesMap()
                .get(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST))));
    attributes.put(
        EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT),
        createAttributeValue(
            backend
                .getAttributesMap()
                .get(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT))));
    attributes.put(
        EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL),
        createAttributeValue(
            backend
                .getAttributesMap()
                .get(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))));

    AttributeValue path =
        backend
            .getAttributesMap()
            .get(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH));
    if (path != null) {
      attributes.put(
          EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH),
          createAttributeValue(path));
    }

    return attributes;
  }

  @Nullable
  private Entity createBackendIfMissing(Entity backendEntity) {
    RequestContext requestContext = RequestContext.forTenantId(backendEntity.getTenantId());
    try {
      Optional<Entity> backendFromCache =
          entityCache
              .getBackendIdAttrsToEntityCache()
              .get(requestContext.buildContextualKey(backendEntity.getIdentifyingAttributesMap()));
      return backendFromCache.orElseGet(
          () -> {
            Entity result = this.upsertBackend(backendEntity);
            LOGGER.info("Created backend:{}", result);
            return result;
          });
    } catch (ExecutionException ex) {
      LOGGER.error("Error trying to load backend from cache for backend:{}", backendEntity);
      return null;
    }
  }

  private org.hypertrace.core.datamodel.AttributeValue createAttributeValue(AttributeValue attr) {
    return AttributeValueCreator.create(attr.getValue().getString());
  }

  @VisibleForTesting
  public Optional<BackendInfo> resolve(
      Event event, StructuredTrace trace, StructuredTraceGraph structuredTraceGraph) {
    for (BackendProvider backendProvider : getBackendProviders()) {
      backendProvider.init(event);

      if (!backendProvider.isValidBackend(event)) {
        continue;
      }

      BackendType type = backendProvider.getBackendType(event);
      Optional<String> maybeBackendUri = backendProvider.getBackendUri(event, structuredTraceGraph);
      if (maybeBackendUri.isEmpty() || StringUtils.isEmpty(maybeBackendUri.get())) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Unable to infer backend uri from event {} for backend type {}",
              HexUtils.getHex(event.getEventId()),
              type);
        }
        continue;
      }

      String backendUri = maybeBackendUri.get();
      final Builder entityBuilder = getBackendEntityBuilder(type, backendUri, event, trace);
      backendProvider.getEntityAttributes(event).forEach(entityBuilder::putAttributes);

      Map<String, org.hypertrace.core.datamodel.AttributeValue> enrichedAttributes =
          new HashMap<>();
      Optional<String> backendOperation = backendProvider.getBackendOperation(event);
      Optional<String> backendDestination = backendProvider.getBackendDestination(event);
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

    return Optional.empty();
  }

  protected Builder getBackendEntityBuilder(
      BackendType type, String backendURI, Event event, StructuredTrace trace) {
    String[] hostAndPort = backendURI.split(COLON);
    String host = hostAndPort[0];
    String port = hostAndPort.length == 2 ? hostAndPort[1] : DEFAULT_PORT;
    String fqn = fqnResolver.resolve(host, event);
    String entityName = port.equals(DEFAULT_PORT) ? fqn : COLON_JOINER.join(fqn, port);
    final Builder entityBuilder =
        Entity.newBuilder()
            .setEntityType(EntityType.BACKEND.name())
            .setTenantId(event.getCustomerId())
            .setEntityName(entityName)
            .putIdentifyingAttributes(
                BACKEND_PROTOCOL_ATTR_NAME, EnricherUtil.createAttributeValue(type.name()))
            .putIdentifyingAttributes(
                BACKEND_HOST_ATTR_NAME, EnricherUtil.createAttributeValue(fqn))
            .putIdentifyingAttributes(
                BACKEND_PORT_ATTR_NAME, EnricherUtil.createAttributeValue(port));
    entityBuilder.putAttributes(
        EnrichedSpanConstants.getValue(Backend.BACKEND_FROM_EVENT),
        EnricherUtil.createAttributeValue(event.getEventName()));
    entityBuilder.putAttributes(
        EnrichedSpanConstants.getValue(Backend.BACKEND_FROM_EVENT_ID),
        EnricherUtil.createAttributeValue(HexUtils.getHex(event.getEventId())));
    return entityBuilder;
  }

  protected Entity upsertBackend(Entity backendEntity) {
    return edsClient.upsert(backendEntity);
  }

  protected Optional<Entity> getPossibleService(
      StructuredTrace trace, Event span, String possibleFqn) {
    return entityCache
        .getFqnToServiceEntityCache()
        .getUnchecked(Pair.of(span.getCustomerId(), possibleFqn));
  }
}
