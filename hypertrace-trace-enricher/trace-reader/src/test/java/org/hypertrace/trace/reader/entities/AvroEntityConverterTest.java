package org.hypertrace.trace.reader.entities;

import static org.hypertrace.trace.reader.attributes.AvroUtil.buildAttributeValue;
import static org.hypertrace.trace.reader.attributes.AvroUtil.buildAttributeValueList;
import static org.hypertrace.trace.reader.attributes.AvroUtil.buildAttributeValueMap;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.booleanAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.booleanListAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.doubleAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.doubleListAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.longAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.longListAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.stringAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.stringListAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.stringMapAttributeValue;
import static org.hypertrace.trace.reader.entities.AvroEntityConverter.convertToAvroEntity;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Entity;
import org.junit.jupiter.api.Test;

class AvroEntityConverterTest {
  private static final String TENANT_ID = "tenant-id";
  private static final org.hypertrace.entity.data.service.v1.Entity BASIC_ENTITY =
      org.hypertrace.entity.data.service.v1.Entity.newBuilder()
          .setEntityType("entity-type")
          .setEntityId("entity-id")
          .setEntityName("entity-name")
          .build();

  private static final Entity BASIC_AVRO_ENTITY =
      Entity.newBuilder()
          .setEntityType("entity-type")
          .setEntityId("entity-id")
          .setEntityName("entity-name")
          .setCustomerId(TENANT_ID)
          .build();

  @Test
  void convertsPrimitives() {
    org.hypertrace.entity.data.service.v1.Entity inputEntity =
        BASIC_ENTITY.toBuilder()
            .putAttributes("string", stringAttributeValue("string-value"))
            .putAttributes("long", longAttributeValue(42))
            .putAttributes("double", doubleAttributeValue(10.2))
            .putAttributes("boolean", booleanAttributeValue(true))
            .build();

    Entity expectedAvroEntity =
        Entity.newBuilder(BASIC_AVRO_ENTITY)
            .setAttributesBuilder(
                Attributes.newBuilder()
                    .setAttributeMap(
                        Map.of(
                            "string",
                            buildAttributeValue("string-value"),
                            "long",
                            buildAttributeValue("42"),
                            "double",
                            buildAttributeValue("10.2"),
                            "boolean",
                            buildAttributeValue("true"))))
            .build();

    assertEquals(expectedAvroEntity, convertToAvroEntity(TENANT_ID, inputEntity).blockingGet());
  }

  @Test
  void convertsAttributeLists() {
    org.hypertrace.entity.data.service.v1.Entity inputEntity =
        BASIC_ENTITY.toBuilder()
            .putAttributes("string-list", stringListAttributeValue("s-v-1", "s-v-2"))
            .putAttributes("long-list", longListAttributeValue(42L, 30L))
            .putAttributes("double-list", doubleListAttributeValue(10.2, 30.3))
            .putAttributes("boolean-list", booleanListAttributeValue(true, false))
            .build();

    Entity expectedAvroEntity =
        Entity.newBuilder(BASIC_AVRO_ENTITY)
            .setAttributesBuilder(
                Attributes.newBuilder()
                    .setAttributeMap(
                        Map.of(
                            "string-list",
                            buildAttributeValueList(List.of("s-v-1", "s-v-2")),
                            "long-list",
                            buildAttributeValueList(List.of("42", "30")),
                            "double-list",
                            buildAttributeValueList(List.of("10.2", "30.3")),
                            "boolean-list",
                            buildAttributeValueList(List.of("true", "false")))))
            .build();

    assertEquals(expectedAvroEntity, convertToAvroEntity(TENANT_ID, inputEntity).blockingGet());
  }

  @Test
  void convertsAttributeMaps() {
    org.hypertrace.entity.data.service.v1.Entity inputEntity =
        BASIC_ENTITY.toBuilder()
            .putAttributes("map", stringMapAttributeValue(Map.of("map-key-1", "map-value-1")))
            .build();

    Entity expectedAvroEntity =
        Entity.newBuilder(BASIC_AVRO_ENTITY)
            .setAttributesBuilder(
                Attributes.newBuilder()
                    .setAttributeMap(
                        Map.of("map", buildAttributeValueMap(Map.of("map-key-1", "map-value-1")))))
            .build();

    assertEquals(expectedAvroEntity, convertToAvroEntity(TENANT_ID, inputEntity).blockingGet());
  }
}
