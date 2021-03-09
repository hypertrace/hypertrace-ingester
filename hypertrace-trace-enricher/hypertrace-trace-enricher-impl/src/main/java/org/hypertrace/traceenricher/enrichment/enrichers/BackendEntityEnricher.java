package org.hypertrace.traceenricher.enrichment.enrichers;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.semantic.convention.constants.db.OTelDbSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.messaging.OtelMessagingSemanticConventions;
import org.hypertrace.entity.constants.v1.ApiAttribute;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.BackendEntityResolver;
import org.hypertrace.traceenricher.util.EntityAvroConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enricher which gets the backend entities from trace event and store backend entity into
 * EntityDataService.
 */
public class BackendEntityEnricher extends AbstractTraceEnricher {
  private static final Logger LOGGER = LoggerFactory.getLogger(BackendEntityEnricher.class);
  private static final String BACKEND_PROTOCOL_ATTR_NAME =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL);
  private static final String BACKEND_HOST_ATTR_NAME =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST);
  List<String> backendOperations = new ArrayList<>(List.of(OTelDbSemanticConventions.DB_OPERATION.getValue(), OtelMessagingSemanticConventions.MESSAGING_OPERATION.getValue()));
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);
  private EdsClient edsClient;
  private EntityCache entityCache;
  private BackendEntityResolver backendEntityResolver;

  @Override
  public void init(Config enricherConfig, ClientRegistry clientRegistry) {
    LOGGER.info("Initialize BackendEntityEnricher with Config: {}", enricherConfig.toString());
    this.edsClient = clientRegistry.getEdsCacheClient();
    this.entityCache = clientRegistry.getEntityCache();
    this.backendEntityResolver = new BackendEntityResolver();
  }

  // At trace level, based on the next span to identify if a backend entity is actually a service
  // entity.
  @Override
  public void enrichTrace(StructuredTrace trace) {
    StructuredTraceGraph structuredTraceGraph = buildGraph(trace);
    trace.getEventList().stream()
        //filter leaf exit spans only
        .filter(event -> EnrichedSpanUtils.isExitSpan(event) &&
            SpanAttributeUtils.isLeafSpan(structuredTraceGraph, event))
        //resolve backend entity
        .map(event -> Pair.of(event, backendEntityResolver.resolveEntity(event, structuredTraceGraph)))
        .filter(pair -> pair.getRight().isPresent())
        //check if backend entity is valid
        .filter(pair -> isValidBackendEntity(pair.getLeft(), pair.getRight().get()))
        //decorate event/trace with backend entity attributes
        .forEach(pair -> decorateWithBackendEntity(pair.getRight().get(), pair.getLeft(), trace));
  }

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    String backendOperation = SpanAttributeUtils.getFirstAvailableStringAttribute(event, backendOperations);
    if (backendOperation != null) {
      addEnrichedAttribute(event, BACKEND_OPERATION_ATTR, AttributeValueCreator.create(backendOperation));
    }
  }

  /**
   * Checks if the candidateEntity is indeed a backend Entity
   */
  private boolean isValidBackendEntity(Event backendSpan, Entity candidateEntity) {
    // Always create backend entity for RabbitMq, Mongo, Redis, Jdbc
    String backendProtocol = candidateEntity
        .getIdentifyingAttributesMap()
        .get(BACKEND_PROTOCOL_ATTR_NAME)
        .getValue()
        .getString();

    BackendType backendType = BackendType.valueOf(backendProtocol);
    if (backendType != BackendType.HTTP && backendType != BackendType.HTTPS &&
        backendType != BackendType.GRPC)
    {
      return true;
    }

    // If there is a Service with the same FQN, then it isn't a Backend
    String fqn = candidateEntity.getIdentifyingAttributesMap()
        .get(BACKEND_HOST_ATTR_NAME)
        .getValue()
        .getString();
    try {
      boolean serviceExists =
          entityCache.getFqnToServiceEntityCache()
              .get(Pair.of(backendSpan.getCustomerId(), fqn))
              .isPresent();
      if (serviceExists) {
        LOGGER.debug("BackendEntity {} is actually an Existing service entity.", candidateEntity);
        return false;
      }
    } catch (ExecutionException ex) {
      LOGGER.error("Error getting service entity using FQN:{} from cache for customerId:{}",
          fqn, backendSpan.getCustomerId());
    }

    // if there's no child, but, it is a partial trace, then check if the destination API
    // is internal to the application
    return !SpanAttributeUtils.containsAttributeKey(backendSpan,
        EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_ID));
    // if it couldn't find a child that's not a service
  }

  private void decorateWithBackendEntity(Entity backendEntity, Event event, StructuredTrace trace) {
    LOGGER.debug("Trying to load or create backend entity: {}, corresponding event: {}",
        backendEntity, event);
    Entity backend = createBackendIfMissing(backendEntity);
    if (backend == null) {
      LOGGER.warn("Failed to upsert backend entity: {}", backendEntity);
      return;
    }
    org.hypertrace.core.datamodel.Entity avroEntity =
        EntityAvroConverter.convertToAvroEntity(backend, true);
    if (avroEntity == null) {
      LOGGER.warn("Error converting backendEntity:{} to avro", backendEntity);
      return;
    }

    addEntity(trace, event, avroEntity);
    addEnrichedAttributes(event, getAttributesToEnrich(backend));
  }

  private List<Pair<String, org.hypertrace.core.datamodel.AttributeValue>> getAttributesToEnrich(
      Entity backend) {
    List<Pair<String, org.hypertrace.core.datamodel.AttributeValue>> attributePairs =
        new ArrayList<>(
            List.of(
                Pair.of(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_ID),
                    AttributeValueCreator.create(backend.getEntityId())),
                Pair.of(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_NAME),
                    AttributeValueCreator.create(backend.getEntityName())),
                Pair.of(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST),
                    createAttributeValue(backend.getAttributesMap().get(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST)))),
                Pair.of(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT),
                    createAttributeValue(backend.getAttributesMap().get(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT)))),
                Pair.of(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL),
                    createAttributeValue(backend.getAttributesMap().get(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL)))),
                Pair.of(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_NAME),
                    AttributeValueCreator.create(backend.getEntityName()))
            )
        );

    AttributeValue path = backend.getAttributesMap().get(
        EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH));
    if (path != null) {
      attributePairs.add(Pair.of(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH),
          createAttributeValue(path)));
    }

    return attributePairs;
  }

  @Nullable
  private Entity createBackendIfMissing(Entity backendEntity) {
    try {
      Optional<Entity> backendFromCache = entityCache.getBackendIdAttrsToEntityCache()
          .get(Pair.of(backendEntity.getTenantId(), backendEntity.getIdentifyingAttributesMap()));
      return backendFromCache.orElseGet(() -> {
        Entity result = edsClient.upsert(backendEntity);
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
}
