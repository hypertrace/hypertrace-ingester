package org.hypertrace.core.kafkastreams.framework.serdes;

import io.confluent.common.utils.Utils;
import java.util.HashMap;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AvroSerdeTest {
  @Test
  public void testAvroSerdeForKeySchemaHeaders() {
    Headers headers = new RecordHeaders();
    headers.add("sample-header-key", Utils.utf8("sample-header-value"));
    final TestRecord serializedRecord = TestRecord.newBuilder().setId(1l).setName("name-1").build();

    AvroSerde serde = new AvroSerde();
    serde.configure(new HashMap<>(), true);
    serde.serializer().serialize("topic-name", headers, serializedRecord);

    Assertions.assertEquals(TestRecord.getClassSchema().toString(),
        Utils.utf8(headers.lastHeader("key.schema").value()));
  }

  @Test
  public void testAvroSerdeForValueSchemaHeaders() {
    Headers headers = new RecordHeaders();
    headers.add("sample-header-key", Utils.utf8("sample-header-value"));
    final TestRecord serializedRecord = TestRecord.newBuilder().setId(1l).setName("name-1").build();

    AvroSerde serde = new AvroSerde();
    serde.configure(new HashMap<>(), false);
    serde.serializer().serialize("topic-name", headers, serializedRecord);

    Assertions.assertEquals(TestRecord.getClassSchema().toString(),
        Utils.utf8(headers.lastHeader("value.schema").value()));
  }

  @Test
  public void testAvroSerde() {
    AvroSerde serde = new AvroSerde();
    Headers headers = new RecordHeaders();
    headers.add("sample-header-key", Utils.utf8("sample-header-value"));
    final TestRecord serializedRecord = TestRecord.newBuilder().setId(1l).setName("name-1").build();

    final byte[] bytes = serde.serializer().serialize("topic-name", headers, serializedRecord);
    final TestRecord deserializedRecord = (TestRecord) serde.deserializer()
        .deserialize("topic-name", headers, bytes);

    Assertions.assertEquals(serializedRecord, deserializedRecord,
        "Serialized record is not matching with deserialized");
    Assertions.assertEquals(serializedRecord.getId(), deserializedRecord.getId());
    Assertions.assertEquals(serializedRecord.getName(), deserializedRecord.getName());
  }
}
