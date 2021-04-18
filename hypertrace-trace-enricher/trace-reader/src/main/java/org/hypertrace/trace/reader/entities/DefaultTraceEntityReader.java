package org.hypertrace.trace.reader.entities;

import static io.reactivex.rxjava3.core.Maybe.zip;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
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
class DefaultTraceEntityReader<T extends GenericRecord, S extends GenericRecord>
    implements TraceEntityReader<T, S> {
  private final EntityTypeClient entityTypeClient;
  private final EntityDataClient entityDataClient;
  private final CachingAttributeClient attributeClient;
  private final TraceAttributeReader<T, S> traceAttributeReader;
  private final Duration writeThrottleDuration;

  DefaultTraceEntityReader(
      EntityTypeClient entityTypeClient,
      EntityDataClient entityDataClient,
      CachingAttributeClient attributeClient,
      TraceAttributeReader<T, S> traceAttributeReader,
      Duration writeThrottleDuration) {
    this.entityTypeClient = entityTypeClient;
    this.entityDataClient = entityDataClient;
    this.attributeClient = attributeClient;
    this.traceAttributeReader = traceAttributeReader;
    this.writeThrottleDuration = writeThrottleDuration;
  }

  @Override
  public Maybe<Entity> getAssociatedEntityForSpan(String entityType, T trace, S span) {
    return spanTenantContext(span)
        .wrapSingle(() -> this.entityTypeClient.get(entityType))
        .flatMapMaybe(
            entityTypeDefinition -> this.getAndWriteEntity(entityTypeDefinition, trace, span));
  }

  @Override
  public Single<Map<String, Entity>> getAssociatedEntitiesForSpan(T trace, S span) {
    return spanTenantContext(span)
        .wrapSingle(
            () ->
                this.entityTypeClient
                    .getAll()
                    .flatMapMaybe(entityType -> this.getAndWriteEntity(entityType, trace, span))
                    .toMap(Entity::getEntityType)
                    .map(Collections::unmodifiableMap));
  }

  private Maybe<Entity> getAndWriteEntity(EntityType entityType, T trace, S span) {
    return this.buildEntity(entityType, trace, span)
        .flatMapSingle(
            entity ->
                this.buildUpsertCondition(entityType, trace, span)
                    .defaultIfEmpty(UpsertCondition.getDefaultInstance())
                    .flatMap(
                        condition ->
                            spanTenantContext(span)
                                .wrapSingle(
                                    () ->
                                        this.entityDataClient.createOrUpdateEntityEventually(
                                            entity, condition, this.writeThrottleDuration))));
  }

  private Maybe<UpsertCondition> buildUpsertCondition(EntityType entityType, T trace, S span) {
    if (entityType.getTimestampAttributeKey().isEmpty()) {
      return Maybe.empty();
    }

    return this.attributeClient
        .get(entityType.getAttributeScope(), entityType.getTimestampAttributeKey())
        .filter(this::isEntitySourced)
        .flatMap(
            attribute ->
                this.buildUpsertCondition(
                    attribute, PredicateOperator.PREDICATE_OPERATOR_LESS_THAN, trace, span));
  }

  private Maybe<UpsertCondition> buildUpsertCondition(
      AttributeMetadata attribute, PredicateOperator operator, T trace, S span) {

    return this.traceAttributeReader
        .getSpanValue(trace, span, attribute.getScopeString(), attribute.getKey())
        .onErrorComplete()
        .flatMap(value -> this.buildUpsertCondition(attribute, operator, value));
  }

  private Maybe<UpsertCondition> buildUpsertCondition(
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

  private Maybe<Entity> buildEntity(EntityType entityType, T trace, S span) {
    Maybe<Map<String, AttributeValue>> attributes =
        this.resolveAllAttributes(entityType.getAttributeScope(), trace, span).cache();

    Maybe<String> id =
        attributes.mapOptional(map -> this.extractString(map, entityType.getIdAttributeKey()));

    Maybe<String> name =
        attributes.mapOptional(map -> this.extractString(map, entityType.getNameAttributeKey()));

    return zip(
            id,
            name,
            attributes,
            (resolvedId, resolvedName, resolvedAttributeMap) ->
                Entity.newBuilder()
                    .setEntityId(resolvedId)
                    .setEntityType(entityType.getName())
                    .setEntityName(resolvedName)
                    .putAllAttributes(resolvedAttributeMap)
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

  private Maybe<Map<String, AttributeValue>> resolveAllAttributes(String scope, T trace, S span) {
    return spanTenantContext(span)
        .wrapSingle(() -> this.attributeClient.getAllInScope(scope))
        .flattenAsObservable(list -> list)
        .filter(this::isEntitySourced)
        .flatMapMaybe(attributeMetadata -> this.resolveAttribute(attributeMetadata, trace, span))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue))
        .toMaybe();
  }

  private Maybe<Entry<String, AttributeValue>> resolveAttribute(
      AttributeMetadata attributeMetadata, T trace, S span) {
    return this.traceAttributeReader
        .getSpanValue(trace, span, attributeMetadata.getScopeString(), attributeMetadata.getKey())
        .onErrorComplete()
        .flatMap(AttributeValueConverter::convertToAttributeValue)
        .map(value -> Map.entry(attributeMetadata.getKey(), value));
  }

  private Optional<String> extractString(
      Map<String, AttributeValue> attributeValueMap, String key) {
    return Optional.ofNullable(attributeValueMap.get(key))
        .filter(value -> value.getTypeCase().equals(TypeCase.VALUE))
        .map(AttributeValue::getValue)
        .filter(value -> value.getTypeCase().equals(Value.TypeCase.STRING))
        .map(Value::getString);
  }

  private GrpcRxExecutionContext spanTenantContext(S span) {
    return GrpcRxExecutionContext.forTenantContext(traceAttributeReader.getTenantId(span));
  }

  private boolean isEntitySourced(AttributeMetadata attributeMetadata) {
    return attributeMetadata.getSourcesList().contains(AttributeSource.EDS);
  }
}
