package org.hypertrace.traceenricher.util;

import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.entity.data.service.v1.EnrichedEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to convert between proto entity to Avro entity attributes. This could go away once
 * we fully move the EDS to AVRO based DTOs.
 */
public class EnrichedEntityAvroConverter {
  private static final Logger LOG = LoggerFactory.getLogger(EnrichedEntityAvroConverter.class);

  public static Entity convertToAvroEntity(EnrichedEntity entity) {
    return Entity.newBuilder()
        .setEntityType(entity.getEntityType())
        .setEntityId(entity.getEntityId())
        .setCustomerId(entity.getTenantId())
        .setEntityName(entity.getEntityName())
        .setAttributes(Attributes.newBuilder().setAttributeMap(getAvroAttributeMap(entity)).build())
        .build();
  }

  /** Converts the attributes of the given proto based entity into AVRO attribute map. */
  public static Map<String, AttributeValue> getAvroAttributeMap(EnrichedEntity entity) {
    Map<String, AttributeValue> attributeMap = new HashMap<>();

    // Convert the proto attributes to Avro based entity attributes.
    for (Map.Entry<String, org.hypertrace.entity.data.service.v1.AttributeValue> entry :
        entity.getAttributesMap().entrySet()) {
      org.hypertrace.entity.data.service.v1.AttributeValue value = entry.getValue();
      AttributeValue result = null;
      if (value.getTypeCase()
          == org.hypertrace.entity.data.service.v1.AttributeValue.TypeCase.VALUE) {
        switch (value.getValue().getTypeCase()) {
          case STRING:
            result = AttributeValue.newBuilder().setValue(value.getValue().getString()).build();
            break;
          case INT:
            result =
                AttributeValue.newBuilder()
                    .setValue(String.valueOf(value.getValue().getInt()))
                    .build();
            break;
          case LONG:
            result =
                AttributeValue.newBuilder()
                    .setValue(String.valueOf(value.getValue().getLong()))
                    .build();
            break;
          case BYTES:
            result =
                AttributeValue.newBuilder()
                    .setBinaryValue(value.getValue().getBytes().asReadOnlyByteBuffer())
                    .build();
            break;
          case DOUBLE:
            result =
                AttributeValue.newBuilder()
                    .setValue(String.valueOf(value.getValue().getDouble()))
                    .build();
            break;
          case FLOAT:
            result =
                AttributeValue.newBuilder()
                    .setValue(String.valueOf(value.getValue().getFloat()))
                    .build();
            break;
          case TIMESTAMP:
            result =
                AttributeValue.newBuilder()
                    .setValue(String.valueOf(value.getValue().getTimestamp()))
                    .build();
            break;
          case BOOLEAN:
            result =
                AttributeValue.newBuilder()
                    .setValue(String.valueOf(value.getValue().getBoolean()))
                    .build();
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
