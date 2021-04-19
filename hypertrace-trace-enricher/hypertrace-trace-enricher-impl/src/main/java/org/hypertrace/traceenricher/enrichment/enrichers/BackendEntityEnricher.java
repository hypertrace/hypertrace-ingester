package org.hypertrace.traceenricher.enrichment.enrichers;

import com.typesafe.config.Config;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.semantic.convention.utils.span.SpanSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.BackendInfo;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.BackendResolver;
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
  private EdsClient edsClient;
  private EntityCache entityCache;
  private BackendResolver backendResolver;

  @Override
  public void init(Config enricherConfig, ClientRegistry clientRegistry) {
    LOGGER.info("Initialize BackendEntityEnricher with Config: {}", enricherConfig.toString());
    this.edsClient = clientRegistry.getEdsCacheClient();
    this.entityCache = clientRegistry.getEntityCache();
    this.backendResolver = new BackendResolver();
  }

  // At trace level, based on the next span to identify if a backend entity is actually a service
  // entity.
  @Override
  public void enrichTrace(StructuredTrace trace) {
    StructuredTraceGraph structuredTraceGraph = buildGraph(trace);
    trace.getEventList().stream()
        // filter leaf exit spans only
        .filter(
            event ->
                EnrichedSpanUtils.isExitSpan(event)
                    && SpanAttributeUtils.isLeafSpan(structuredTraceGraph, event))
        // resolve backend entity
        .map(event -> Pair.of(event, backendResolver.resolve(event, structuredTraceGraph)))
        .filter(pair -> pair.getRight().isPresent())
        // check if backend entity is valid
        .filter(pair -> isValidBackendEntity(pair.getLeft(), pair.getRight().get()))
        // decorate event/trace with backend entity attributes
        .forEach(pair -> decorateWithBackendEntity(pair.getRight().get(), pair.getLeft(), trace));
  }

  /** Checks if the candidateEntity is indeed a backend Entity */
  private boolean isValidBackendEntity(Event backendSpan, BackendInfo candidateInfo) {
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

    if (checkIfServiceEntityExists(backendSpan.getCustomerId(), fqn, candidateInfo.getEntity())) {
      return false;
    }

    // checks the existence of peer service in case if it is a partial trace, and we are missing
    // its immediate child span.
    String peerServiceName = SpanSemanticConventionUtils.getPeerServiceName(backendSpan);
    if (peerServiceName != null
        && checkIfServiceEntityExists(
            backendSpan.getCustomerId(), peerServiceName, candidateInfo.getEntity())) {
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
      String tenantId, String serviceFqn, Entity candidateBackendEntity) {
    try {
      boolean serviceExists =
          entityCache.getFqnToServiceEntityCache().get(Pair.of(tenantId, serviceFqn)).isPresent();
      if (serviceExists) {
        LOGGER.debug(
            "BackendEntity {} is actually an Existing service entity.", candidateBackendEntity);
      }
      return serviceExists;
    } catch (ExecutionException ex) {
      LOGGER.error(
          "Error getting service entity using FQN:{} from cache for customerId:{}",
          serviceFqn,
          tenantId);
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
    try {
      Optional<Entity> backendFromCache =
          entityCache
              .getBackendIdAttrsToEntityCache()
              .get(
                  Pair.of(
                      backendEntity.getTenantId(), backendEntity.getIdentifyingAttributesMap()));
      return backendFromCache.orElseGet(
          () -> {
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
