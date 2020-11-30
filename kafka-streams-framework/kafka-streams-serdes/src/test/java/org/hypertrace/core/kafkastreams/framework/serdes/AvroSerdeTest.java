package org.hypertrace.core.kafkastreams.framework.serdes;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AvroSerdeTest {

  @Test
  public void testSpecificAvroSerde() {
    AvroSerde serde = new AvroSerde();
    final TestRecord serializedRecord = TestRecord.newBuilder().setId(1l).setName("name-1").build();

    final byte[] bytes = serde.serializer().serialize("topic-name", serializedRecord);
    final TestRecord deserializedRecord = (TestRecord) serde.deserializer()
        .deserialize("topic-name", bytes);

    Assertions.assertEquals(serializedRecord, deserializedRecord,
        "Serialized record is not matching with deserialized");
    Assertions.assertEquals(serializedRecord.getId(), deserializedRecord.getId());
    Assertions.assertEquals(serializedRecord.getName(), deserializedRecord.getName());
  }

  @Test
  public void testSpecificAvroSerdeForNullValue() {
    AvroSerde serde = new AvroSerde();

    final byte[] bytes = serde.serializer().serialize("topic-name", null);
    final TestRecord deserializedRecord = (TestRecord) serde.deserializer()
        .deserialize("topic-name", bytes);

    Assertions.assertNull(deserializedRecord);
  }

  @Test
  public void testGenericAvroSerde() {
    GenericAvroSerde serde = new GenericAvroSerde();
    final TestRecord serializedRecord = TestRecord.newBuilder().setId(1l).setName("name-1").build();

    final byte[] bytes = serde.serializer().serialize("topic-name", serializedRecord);
    final Record deserializedRecord = (Record) serde.deserializer().deserialize("topic-name", bytes);

    Assertions.assertEquals(serializedRecord.getId(), deserializedRecord.get("id"));
    Assertions.assertEquals(serializedRecord.getName(), deserializedRecord.get("name"));
  }

  @Test
  public void testGenericAvroSerdeForNullValue() {
    AvroSerde serde = new AvroSerde();

    final byte[] bytes = serde.serializer().serialize("topic-name", null);
    final TestRecord deserializedRecord = (TestRecord) serde.deserializer()
        .deserialize("topic-name", bytes);

    Assertions.assertNull(deserializedRecord);
  }

}
