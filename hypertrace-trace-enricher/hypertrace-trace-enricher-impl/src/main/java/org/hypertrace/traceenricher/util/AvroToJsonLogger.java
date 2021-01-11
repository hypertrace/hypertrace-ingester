package org.hypertrace.traceenricher.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;

/**
 * Handy class to log an avro object in JSON format that can be deserialized back into the object
 * for debugging. It checks if debug log is enabled first.
 */
public class AvroToJsonLogger {
  public static <T extends SpecificRecordBase> String convertToJsonString(T object, Schema schema)
      throws IOException {
    JsonEncoder encoder;
    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      DatumWriter<T> writer = new SpecificDatumWriter<>(schema);
      encoder = EncoderFactory.get().jsonEncoder(schema, output, false);
      writer.write(object, encoder);
      encoder.flush();
      output.flush();
      return new String(output.toByteArray());
    }
  }

  public static <T extends SpecificRecordBase> void log(Logger logger, String fmt, T object) {
    if (logger.isDebugEnabled()) {
      try {
        logger.debug(fmt, convertToJsonString(object, object.getSchema()));
      } catch (IOException e) {
        logger.warn("An exception occured while converting avro to JSON string", e);
      }
    }
  }
}
