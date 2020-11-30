package org.hypertrace.core.kafkastreams.framework.serdes;

import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

public class AvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {
  public static final int INITIAL_SIZE = 16 * 1024;

  @Override
  public byte[] serialize(String topic, T data) {
    if(data == null) {
      return new byte[0];
    }

    Schema writerSchema = data.getSchema();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(INITIAL_SIZE);
    SpecificDatumWriter<T> datumWriter = new SpecificDatumWriter<>(writerSchema);
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