package org.hypertrace.core.kafkastreams.framework.serdes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
  @Override
  public T deserialize(String topic, byte[] data) {
    if(data == null || data.length == 0) {
      return null;
    }
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(data);
      BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(bais, null);
      Schema writerSchema = new Parser().parse(binaryDecoder.readString());
      final Schema readerSchema = getSpecificReaderSchema(writerSchema);
      SpecificDatumReader<T> reader = new SpecificDatumReader<>(writerSchema, readerSchema);
      return reader.read(null, binaryDecoder);
    } catch (IOException e) {
      throw new RuntimeException("Deserialization error", e);
    }
  }

  private Schema getSpecificReaderSchema(Schema writerSchema) {
    Class<SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);
    if (readerClass == null) {
      throw new SerializationException("Could not find class: " + writerSchema.getFullName());
    } else {
      try {
        return readerClass.newInstance().getSchema();
      } catch (Exception e) {
        throw new SerializationException("Error while serializing: " + writerSchema.getFullName(),
            e);
      }
    }
  }
}
