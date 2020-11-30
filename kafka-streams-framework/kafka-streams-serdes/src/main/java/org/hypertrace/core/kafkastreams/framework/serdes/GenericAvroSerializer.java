package org.hypertrace.core.kafkastreams.framework.serdes;

import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;

public class GenericAvroSerializer<T extends GenericRecord> implements Serializer<T> {
  public static final int INITIAL_SIZE = 16 * 1024;

  @Override
  public byte[] serialize(String topic, T data) {
    if(data == null) {
      return new byte[0];
    }

    Schema writerSchema = data.getSchema();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(INITIAL_SIZE);
    GenericDatumWriter<T> datumWriter = new GenericDatumWriter<>(writerSchema);
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
    try {
      encoder.writeString(writerSchema.toString());
      datumWriter.write(data, encoder);
      encoder.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return baos.toByteArray();
  }
}
