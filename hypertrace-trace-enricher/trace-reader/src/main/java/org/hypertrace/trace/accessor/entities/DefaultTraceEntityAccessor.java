package org.hypertrace.trace.accessor.entities;

import static java.util.function.Predicate.not;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.grpcutils.client.rx.GrpcRxExecutionContext;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValue.TypeCase;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate.PredicateOperator;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.entity.type.service.v2.EntityType.EntityFormationCondition;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;

@Slf4j
class DefaultTraceEntityAccessor implements TraceEntityAccessor {
  private final EntityTypeClient entityTypeClient;
  private final EntityDataClient entityDataClient;
  private final AttributeServiceCachedClient attributeClient;
  private final TraceAttributeReader<StructuredTrace, Event> traceAttributeReader;
  private final Duration writeThrottleDuration;
  private final Set<String> excludedEntityTypes;

  DefaultTraceEntityAccessor(
      EntityTypeClient entityTypeClient,
      EntityDataClient entityDataClient,
      AttributeServiceCachedClient attributeClient,
      TraceAttributeReader<StructuredTrace, Event> traceAttributeReader,
      Duration writeThrottleDuration,
      Set<String> excludedEntityTypes) {
    this.entityTypeClient = entityTypeClient;
    this.entityDataClient = entityDataClient;
    this.attributeClient = attributeClient;
    this.traceAttributeReader = traceAttributeReader;
    this.writeThrottleDuration = writeThrottleDuration;
    this.excludedEntityTypes = excludedEntityTypes;
  }

  @Override
  public void writeAssociatedEntitiesForSpanEventually(StructuredTrace trace, Event span) {
    this.spanTenantContext(span)
        .wrapSingle(() -> this.entityTypeClient.getAll().toList())
        .blockingGet()
        .stream()
        .filter(not(this::isExcludedEntityType))
        .forEach(entityType -> this.writeEntityIfExists(entityType, trace, span));
  }

  private boolean isExcludedEntityType(final EntityType entityType) {
    return excludedEntityTypes.contains(entityType.getName());
  }

  private void writeEntityIfExists(EntityType entityType, StructuredTrace trace, Event span) {
    this.buildEntity(entityType, trace, span)
        .map(
            entity -> {
              this.entityDataClient.createOrUpdateEntityEventually(
                  this.traceAttributeReader.getRequestContext(span),
                  entity,
                  this.buildUpsertCondition(entityType, trace, span)
                      .orElse(UpsertCondition.getDefaultInstance()),
                  this.writeThrottleDuration);
              return null;
            });
  }

  private Optional<UpsertCondition> buildUpsertCondition(
      EntityType entityType, StructuredTrace trace, Event span) {
    if (entityType.getTimestampAttributeKey().isEmpty()) {
      return Optional.empty();
    }
    return this.attributeClient
        .get(
            traceAttributeReader.getRequestContext(span),
            entityType.getAttributeScope(),
            entityType.getTimestampAttributeKey())
        .filter(this::isEntitySourced)
        .flatMap(
            attribute ->
                this.buildUpsertCondition(
                    attribute, PredicateOperator.PREDICATE_OPERATOR_LESS_THAN, trace, span));
  }

  private Optional<UpsertCondition> buildUpsertCondition(
      AttributeMetadata attribute, PredicateOperator operator, StructuredTrace trace, Event span) {
    return this.traceAttributeReader
        .getSpanValue(trace, span, attribute.getScopeString(), attribute.getKey())
        .flatMap(value -> this.buildUpsertCondition(attribute, operator, value));
  }

  private Optional<UpsertCondition> buildUpsertCondition(
      AttributeMetadata attribute, PredicateOperator operator, LiteralValue currentValue) {
    return AttributeValueConverter.convertToAttributeValue(currentValue)
        .map(
            attributeValue ->
                UpsertCondition.newBuilder()
                    .setPropertyPredicate(
                        Predicate.newBuilder()
                            .setAttributeKey(attribute.getKey())
                            .setOperator(operator)
                            .setValue(attributeValue))
                    .build());
  }

  private Optional<Entity> buildEntity(EntityType entityType, StructuredTrace trace, Event span) {
    Optional<Map<String, AttributeValue>> attributes =
        this.resolveAllAttributes(entityType.getAttributeScope(), trace, span);
    Optional<String> id =
        attributes.flatMap(map -> this.extractNonEmptyString(map, entityType.getIdAttributeKey()));
    Optional<String> name =
        attributes.flatMap(
            map -> this.extractNonEmptyString(map, entityType.getNameAttributeKey()));
    if (id.isEmpty() || name.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
            Entity.newBuilder()
                .setEntityId(id.get())
                .setEntityType(entityType.getName())
                .setEntityName(name.get())
                .putAllAttributes(attributes.get())
                .build())
        .filter(entity -> this.canCreateEntity(entityType, entity));
  }

  private boolean canCreateEntity(EntityType entityType, Entity entity) {
    return entityType.getRequiredConditionsList().stream()
        .allMatch(condition -> this.passesFormationCondition(entity, condition));
  }

  private boolean passesFormationCondition(Entity entity, EntityFormationCondition condition) {
    switch (condition.getConditionCase()) {
      case REQUIRED_KEY:
        return entity.getAttributesMap().containsKey(condition.getRequiredKey());
      case CONDITION_NOT_SET: // No condition should not filter formation
        return true;
      default: // Unrecognized condition
        log.error("Unrecognized formation condition: {}", condition);
        return false;
    }
  }

  private Optional<Map<String, AttributeValue>> resolveAllAttributes(
      String scope, StructuredTrace trace, Event span) {
    List<AttributeMetadata> attributeMetadataList =
        this.attributeClient.getAllInScope(
            this.traceAttributeReader.getRequestContext(span), scope);
    Map<String, AttributeValue> resolvedAttributes =
        attributeMetadataList.stream()
            .filter(this::isEntitySourced)
            .map(attributeMetadata -> this.resolveAttribute(attributeMetadata, trace, span))
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
    if (resolvedAttributes.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(resolvedAttributes);
  }

  private Optional<Entry<String, AttributeValue>> resolveAttribute(
      AttributeMetadata attributeMetadata, StructuredTrace trace, Event span) {
    return this.traceAttributeReader
        .getSpanValue(trace, span, attributeMetadata.getScopeString(), attributeMetadata.getKey())
        .flatMap(AttributeValueConverter::convertToAttributeValue)
        .map(value -> Map.entry(attributeMetadata.getKey(), value));
  }

  private Optional<String> extractNonEmptyString(
      Map<String, AttributeValue> attributeValueMap, String key) {
    return Optional.ofNullable(attributeValueMap.get(key))
        .filter(value -> value.getTypeCase().equals(TypeCase.VALUE))
        .map(AttributeValue::getValue)
        .filter(value -> value.getTypeCase().equals(Value.TypeCase.STRING))
        .map(Value::getString)
        .filter(not(String::isEmpty));
  }

  private GrpcRxExecutionContext spanTenantContext(Event span) {
    return GrpcRxExecutionContext.forTenantContext(traceAttributeReader.getTenantId(span));
  }

  private boolean isEntitySourced(AttributeMetadata attributeMetadata) {
    return attributeMetadata.getSourcesList().contains(AttributeSource.EDS);
  }
}
