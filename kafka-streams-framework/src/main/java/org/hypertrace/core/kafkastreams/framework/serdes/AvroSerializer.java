package org.hypertrace.core.kafkastreams.framework.serdes;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;

public class AvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {
  public static final int INITIAL_SIZE = 16 * 1024;
  private boolean isKey;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
  }

  @Override
  public byte[] serialize(String topic, T data) {
    return serialize(topic, null, data);
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T data) {
    if (isKey) {
      headers.add("key.schema", Utils.utf8(data.getSchema().toString()));
    } else {
      headers.add("value.schema", Utils.utf8(data.getSchema().toString()));
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream(INITIAL_SIZE);
    SpecificDatumWriter<T> datumWriter = new SpecificDatumWriter<>(data.getSchema());
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
    try {
      datumWriter.write(data, encoder);
      encoder.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return baos.toByteArray();
  }
}