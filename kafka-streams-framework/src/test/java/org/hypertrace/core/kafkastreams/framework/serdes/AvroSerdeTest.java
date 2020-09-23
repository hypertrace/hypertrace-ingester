package org.hypertrace.core.kafkastreams.framework.serdes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AvroSerdeTest {
  @Test
  public void testAvroSerde() {
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
  public void testAvroSerdeForNullValue() {
    AvroSerde serde = new AvroSerde();

    final byte[] bytes = serde.serializer().serialize("topic-name", null);
    final TestRecord deserializedRecord = (TestRecord) serde.deserializer()
        .deserialize("topic-name", bytes);

    Assertions.assertNull(deserializedRecord);
  }

}
