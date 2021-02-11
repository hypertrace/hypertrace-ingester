package org.hypertrace.trace.reader.entities;

import static io.reactivex.rxjava3.core.Maybe.zip;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.grpcutils.client.rx.GrpcRxExecutionContext;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValue.TypeCase;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;

class DefaultTraceEntityReader<T extends GenericRecord, S extends GenericRecord>
    implements TraceEntityReader<T, S> {

  private final EntityTypeClient entityTypeClient;
  private final EntityDataClient entityDataClient;
  private final CachingAttributeClient attributeClient;
  private final TraceAttributeReader<T, S> traceAttributeReader;

  DefaultTraceEntityReader(
      EntityTypeClient entityTypeClient,
      EntityDataClient entityDataClient,
      CachingAttributeClient attributeClient,
      TraceAttributeReader<T, S> traceAttributeReader) {
    this.entityTypeClient = entityTypeClient;
    this.entityDataClient = entityDataClient;
    this.attributeClient = attributeClient;
    this.traceAttributeReader = traceAttributeReader;
  }

  @Override
  public Maybe<Entity> getAssociatedEntityForSpan(String entityType, T trace, S span) {
    return spanTenantContext(span)
        .wrapSingle(() -> this.entityTypeClient.get(entityType))
        .flatMapMaybe(
            entityTypeDefinition -> this.getOrCreateEntity(entityTypeDefinition, trace, span));
  }

  @Override
  public Single<Map<String, Entity>> getAssociatedEntitiesForSpan(T trace, S span) {

    return spanTenantContext(span)
        .wrapSingle(
            () ->
                this.entityTypeClient
                    .getAll()
                    .flatMapMaybe(entityType -> this.getOrCreateEntity(entityType, trace, span))
                    .toMap(Entity::getEntityType)
                    .map(Collections::unmodifiableMap));
  }

  private Maybe<Entity> getOrCreateEntity(EntityType entityType, T trace, S span) {
    return this.buildEntity(entityType, trace, span)
        .flatMapSingle(
            entity ->
                spanTenantContext(span)
                    .wrapSingle(() -> this.entityDataClient.getOrCreateEntity(entity)));
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
                .build());
  }

  private Maybe<Map<String, AttributeValue>> resolveAllAttributes(String scope, T trace, S span) {
    return spanTenantContext(span)
        .wrapSingle(() -> this.attributeClient.getAllInScope(scope))
        .flattenAsObservable(list -> list)
        .filter(attributeMetadata -> attributeMetadata.getType().equals(AttributeType.ATTRIBUTE))
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
}
