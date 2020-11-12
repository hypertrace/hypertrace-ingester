package org.hypertrace.trace.reader.entities;

import static java.util.Objects.nonNull;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nonnull;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.AttributeValueMap;
import org.hypertrace.entity.data.service.v1.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AvroEntityConverter {

  private static final Logger LOG = LoggerFactory.getLogger(AvroEntityConverter.class);

  public Single<Entity> convertToAvroEntity(
      @Nonnull String tenantId, @Nonnull org.hypertrace.entity.data.service.v1.Entity entity) {
    return this.convertAttributes(entity.getAttributesMap())
        .map(
            attributes ->
                Entity.newBuilder()
                    .setEntityType(entity.getEntityType())
                    .setEntityId(entity.getEntityId())
                    .setCustomerId(tenantId)
                    .setEntityName(entity.getEntityName())
                    .setAttributes(attributes)
                    .build());
  }

  private Single<Attributes> convertAttributes(
      Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> attributeMap) {

    return Observable.fromIterable(attributeMap.entrySet())
        .flatMapMaybe(
            entry ->
                this.convertAttributeValue(entry.getValue())
                    .doOnError(error -> LOG.error("Dropping attribute on conversion", error))
                    .onErrorComplete()
                    .map(value -> Map.entry(entry.getKey(), value)))
        .toMap(Entry::getKey, Entry::getValue)
        .map(convertedMap -> Attributes.newBuilder().setAttributeMap(convertedMap).build());
  }

  private Single<AttributeValue> convertAttributeValue(
      org.hypertrace.entity.data.service.v1.AttributeValue value) {
    switch (value.getTypeCase()) {
      case VALUE:
        return this.convertValue(value.getValue());
      case VALUE_LIST:
        return this.convertValueList(value.getValueList());
      case VALUE_MAP:
        return this.convertValueMap(value.getValueMap());
      case TYPE_NOT_SET:
      default:
        return Single.error(
            new UnsupportedOperationException(
                "Unhandled attribute value type: " + value.getTypeCase()));
    }
  }

  private Single<AttributeValue> convertValueList(AttributeValueList valueList) {
    return Observable.fromIterable(valueList.getValuesList())
        .concatMapSingle(this::convertAttributeValue)
        .switchMapSingle(
            value ->
                nonNull(value.getValue())
                    ? Single.just(value)
                    : Single.error(
                        new UnsupportedOperationException(
                            "Avro value lists do not support nested lists or maps")))
        .map(AttributeValue::getValue) // Unwrap and flatten
        .toList()
        .map(list -> AttributeValue.newBuilder().setValueList(list).build());
  }

  private Single<AttributeValue> convertValueMap(AttributeValueMap valueMap) {
    return Observable.fromIterable(valueMap.getValuesMap().entrySet())
        .concatMapSingle(
            entry ->
                this.convertAttributeValue(entry.getValue())
                    .flatMap(
                        value ->
                            nonNull(value.getValue())
                                ? Single.just(value)
                                : Single.error(
                                    new UnsupportedOperationException(
                                        "Avro value maps do not support nested lists or maps")))
                    .map(AttributeValue::getValue) // Unwrap and flatten
                    .map(value -> Map.entry(entry.getKey(), value)))
        .toMap(Entry::getKey, Entry::getValue)
        .map(map -> AttributeValue.newBuilder().setValueMap(map).build());
  }

  private Single<AttributeValue> convertValue(Value value) {
    switch (value.getTypeCase()) {
      case STRING:
        return this.buildAttributeStringValueSingle(value.getString());
      case INT:
        return this.buildAttributeStringValueSingle(String.valueOf(value.getInt()));
      case LONG:
        return this.buildAttributeStringValueSingle(String.valueOf(value.getLong()));
      case DOUBLE:
        return this.buildAttributeStringValueSingle(String.valueOf(value.getDouble()));
      case FLOAT:
        return this.buildAttributeStringValueSingle(String.valueOf(value.getFloat()));
      case TIMESTAMP:
        return this.buildAttributeStringValueSingle(String.valueOf(value.getTimestamp()));
      case BOOLEAN:
        return this.buildAttributeStringValueSingle(String.valueOf(value.getBoolean()));
      case BYTES:
        return Single.just(
            AttributeValue.newBuilder()
                .setBinaryValue(value.getBytes().asReadOnlyByteBuffer())
                .build());
      case CUSTOM:
      case TYPE_NOT_SET:
      default:
        return Single.error(
            new UnsupportedOperationException(
                "Unhandled entity attribute type: " + value.getTypeCase()));
    }
  }

  private Single<AttributeValue> buildAttributeStringValueSingle(String value) {
    return Single.just(AttributeValue.newBuilder().setValue(value).build());
  }
}
