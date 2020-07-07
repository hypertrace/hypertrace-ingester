package org.hypertrace.traceenricher.util;

import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Entity;
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
        switch (value.getValue().getTypeCase()) {
          case STRING:
            result = AttributeValue.newBuilder().setValue(value.getValue().getString()).build();
            break;
          case INT:
            result = AttributeValue.newBuilder().setValue(String.valueOf(value.getValue().getInt())).build();
            break;
          case LONG:
            result = AttributeValue.newBuilder().setValue(
                String.valueOf(value.getValue().getLong())).build();
            break;
          case BYTES:
            result = AttributeValue.newBuilder().setBinaryValue(
                value.getValue().getBytes().asReadOnlyByteBuffer()).build();
            break;
          case DOUBLE:
            result = AttributeValue.newBuilder().setValue(
                String.valueOf(value.getValue().getDouble())).build();
            break;
          case FLOAT:
            result = AttributeValue.newBuilder().setValue(
                String.valueOf(value.getValue().getFloat())).build();
            break;
          case TIMESTAMP:
            result = AttributeValue.newBuilder().setValue(
                String.valueOf(value.getValue().getTimestamp())).build();
            break;
          case BOOLEAN:
            result = AttributeValue.newBuilder().setValue(
                String.valueOf(value.getValue().getBoolean())).build();
            break;
          default:
            LOG.warn("Unhandled entity attribute type: " + value.getValue().getTypeCase());
        }
      } else {
        // Currently we don't copy the list or map types.
        LOG.warn("Unsupported entity attribute type: " + value);
      }

      if (result != null) {
        attributeMap.put(entry.getKey(), result);
      }
    }

    return attributeMap;
  }
}
