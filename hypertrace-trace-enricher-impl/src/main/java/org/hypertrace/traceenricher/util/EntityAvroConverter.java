package org.hypertrace.traceenricher.util;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.entity.data.service.v1.Value.TypeCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityAvroConverter {
  private static final Logger LOG = LoggerFactory.getLogger(EntityAvroConverter.class);

  @Nullable
  public static Entity convertToAvroEntity(@Nullable org.hypertrace.entity.data.service.v1.Entity entity,
                                           boolean includeAttributes) {
    if (entity == null) {
      return null;
    }

    if (Strings.isNullOrEmpty(entity.getEntityId()) || Strings.isNullOrEmpty(entity.getEntityType())) {
      LOG.warn("Invalid entity: {}", entity);
      return null;
    }

    Entity.Builder builder = Entity.newBuilder().setEntityType(entity.getEntityType())
        .setEntityId(entity.getEntityId())
        .setCustomerId(entity.getTenantId())
        .setEntityName(entity.getEntityName());

    if (includeAttributes) {
      builder.setAttributes(Attributes.newBuilder().setAttributeMap(
          getAvroAttributeMap(entity)).build());
    }
    return builder.build();
  }

  private static Map<String, AttributeValue> getAvroAttributeMap(
      org.hypertrace.entity.data.service.v1.Entity entity) {
    Map<String, AttributeValue> attributeMap = new HashMap<>();

    // Convert the proto attributes to Avro based entity attributes.
    for (Map.Entry<String, org.hypertrace.entity.data.service.v1.AttributeValue> entry :
        entity.getAttributesMap().entrySet()) {
      org.hypertrace.entity.data.service.v1.AttributeValue value = entry.getValue();
      AttributeValue result = null;
      if (value.getTypeCase() == org.hypertrace.entity.data.service.v1.AttributeValue.TypeCase.VALUE) {
        if (value.getValue().getTypeCase() == TypeCase.BYTES) {
          result = AttributeValue.newBuilder().setBinaryValue(value.getValue().getBytes().asReadOnlyByteBuffer()).build();
        } else {
          Optional<String> valueStr = convertValueToString(value.getValue());
          if (valueStr.isPresent()) {
            result = AttributeValue.newBuilder().setValue(valueStr.get()).build();
          }
        }
      } else if (value.getTypeCase() == org.hypertrace.entity.data.service.v1.AttributeValue.TypeCase.VALUE_LIST) {
        org.hypertrace.entity.data.service.v1.AttributeValueList attributeValueList = value.getValueList();
        List<String> avroValuesStringList = new ArrayList<>();
        for (org.hypertrace.entity.data.service.v1.AttributeValue attributeValue : attributeValueList.getValuesList()) {
          if (attributeValue.getTypeCase() == org.hypertrace.entity.data.service.v1.AttributeValue.TypeCase.VALUE) {
            Optional<String> valueStr = convertValueToString(attributeValue.getValue());
            valueStr.ifPresent(avroValuesStringList::add);
          } else {
            LOG.warn("Unsupported entity attribute type of list of lists or list of maps: {}", attributeValue);
          }
        }
        // Only add the list if it's not empty
        if (!avroValuesStringList.isEmpty()) {
          result = AttributeValue.newBuilder().setValueList(avroValuesStringList).build();
        }
      } else {
        // Currently we don't copy the map types.
        LOG.warn("Unsupported entity attribute type: " + value);
      }

      if (result != null) {
        attributeMap.put(entry.getKey(), result);
      }
    }

    return attributeMap;
  }

  private static Optional<String> convertValueToString(org.hypertrace.entity.data.service.v1.Value value) {
    switch (value.getTypeCase()) {
      case STRING:
        return Optional.of(value.getString());
      case INT:
        return Optional.of(String.valueOf(value.getInt()));
      case LONG:
        return Optional.of(String.valueOf(value.getLong()));
      case BYTES:
        return Optional.of(HexUtils.getHex(value.getBytes().asReadOnlyByteBuffer()));
      case DOUBLE:
        return Optional.of(String.valueOf(value.getDouble()));
      case FLOAT:
        return Optional.of(String.valueOf(value.getFloat()));
      case TIMESTAMP:
        return Optional.of(String.valueOf(value.getTimestamp()));
      case BOOLEAN:
        return Optional.of(String.valueOf(value.getBoolean()));
      default:
        LOG.warn("Unhandled entity attribute type: " + value.getTypeCase());
    }
    return Optional.empty();
  }
}
