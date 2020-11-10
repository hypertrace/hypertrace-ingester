package org.hypertrace.traceenricher.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.AttributeValueMap;
import org.hypertrace.entity.data.service.v1.Value;
import org.junit.jupiter.api.Test;

public class EntityAvroConverterTest {
  @Test
  public void testConvertToAvroEntity() {
    Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> attributeValueMap = new HashMap<>();

    attributeValueMap.put("attr1", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("v1")).build());
    attributeValueMap.put("attr2", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setBoolean(true)).build());
    attributeValueMap.put("attr3", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setInt(23)).build());
    attributeValueMap.put("attr4", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValueList(
        AttributeValueList.newBuilder()
            .addValues(org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l1")))
            .addValues(org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l2")))
            .addValues(org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l3")))
            .addValues(org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setBytes(ByteString.copyFrom("l4".getBytes()))))
    ).build());
    attributeValueMap.put("attr5", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setBytes(ByteString.copyFrom("test-bytes".getBytes()))).build());
    attributeValueMap.put("attr6", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setDouble(33.0)).build());
    attributeValueMap.put("attr7", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setFloat(18.0f)).build());
    attributeValueMap.put("attr8", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setTimestamp(46)).build());
    attributeValueMap.put("attr9", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder()).build());
    attributeValueMap.put("attr10", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValueMap(
        AttributeValueMap.newBuilder().putAllValues(
            Map.of("k1", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("v11")).build(),
                "k2", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("v12")).build())
        )
    ).build());
    attributeValueMap.put("attr11", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setLong(37)).build());

    org.hypertrace.entity.data.service.v1.Entity entity = org.hypertrace.entity.data.service.v1.Entity.newBuilder()
        .setEntityId("entity-id")
        .setEntityName("entity-name")
        .setEntityType("entity-type")
        .setTenantId("entity-tenant-id")
        .putAllAttributes(attributeValueMap)
        .build();

    Entity avroEntity1 = EntityAvroConverter.convertToAvroEntity(entity, false);
    assertEquals(
        Entity.newBuilder()
            .setEntityId("entity-id")
            .setEntityName("entity-name")
            .setEntityType("entity-type")
            .setCustomerId("entity-tenant-id")
            .build(),
        avroEntity1);

    Entity avroEntity2 = EntityAvroConverter.convertToAvroEntity(entity, true);
    assertEquals(
        Entity.newBuilder()
            .setEntityId("entity-id")
            .setEntityName("entity-name")
            .setEntityType("entity-type")
            .setCustomerId("entity-tenant-id")
            .setAttributesBuilder(
                Attributes.newBuilder()
                    .setAttributeMap(Map.of(
                        "attr1", AttributeValue.newBuilder().setValue("v1").build(),
                        "attr2", AttributeValue.newBuilder().setValue("true").build(),
                        "attr3", AttributeValue.newBuilder().setValue("23").build(),
                        "attr4", AttributeValue.newBuilder().setValueList(List.of("l1", "l2", "l3", HexUtils.getHex("l4".getBytes()))).build(),
                        "attr5", AttributeValue.newBuilder().setBinaryValue(ByteBuffer.wrap("test-bytes".getBytes())).build(),
                        "attr6", AttributeValue.newBuilder().setValue("33.0").build(),
                        "attr7", AttributeValue.newBuilder().setValue("18.0").build(),
                        "attr8", AttributeValue.newBuilder().setValue("46").build(),
                        "attr11", AttributeValue.newBuilder().setValue("37").build()
                    ))
            )
            .build(),
        avroEntity2);
  }

  @Test
  public void testListOfListsNotConverted() {
    org.hypertrace.entity.data.service.v1.Entity entity = org.hypertrace.entity.data.service.v1.Entity.newBuilder()
        .setEntityId("entity-id")
        .setEntityName("entity-name")
        .setEntityType("entity-type")
        .setTenantId("entity-tenant-id")
        .putAllAttributes(Map.of(
            "attr1", org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValueList(
                AttributeValueList.newBuilder()
                    .addValues(org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValueList(
                        AttributeValueList.newBuilder()
                            .addValues(org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l1")))
                            .addValues(org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l2")))
                            .addValues(org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l3")))
                    )).build()
            ).build()
        ))
        .build();

    Entity avroEntity1 = EntityAvroConverter.convertToAvroEntity(entity, true);
    assertEquals(
        Entity.newBuilder()
            .setEntityId("entity-id")
            .setEntityName("entity-name")
            .setEntityType("entity-type")
            .setCustomerId("entity-tenant-id")
            .setAttributesBuilder(
                Attributes.newBuilder()
                    .setAttributeMap(Map.of())
            )
            .build(),
        avroEntity1);
  }
}
