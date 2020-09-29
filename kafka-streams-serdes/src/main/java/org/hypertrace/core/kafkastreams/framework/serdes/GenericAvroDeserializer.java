package org.hypertrace.core.kafkastreams.framework.serdes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;

public class GenericAvroDeserializer<T extends GenericRecord> implements Deserializer<T> {
  @Override
  public T deserialize(String topic, byte[] data) {
    if(data == null || data.length == 0) {
      return null;
    }
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(data);
      BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(bais, null);
      Schema writerSchema = new Parser().parse(binaryDecoder.readString());

      GenericDatumReader<T> reader = new GenericDatumReader<>(writerSchema, writerSchema);
      return reader.read(null, binaryDecoder);
    } catch (IOException e) {
      throw new RuntimeException("Deserialization error", e);
    }
  }
}
