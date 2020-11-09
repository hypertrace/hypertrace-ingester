package org.hypertrace.core.spannormalizer.jaeger;

import com.google.protobuf.InvalidProtocolBufferException;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JaegerSpanSerde implements Serde<Span> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public void close() {
  }

  @Override
  public Serializer<Span> serializer() {
    return new Ser();
  }

  @Override
  public Deserializer<Span> deserializer() {
    return new De();
  }

  public static class Ser implements Serializer<Span> {

    @Override
    public byte[] serialize(String topic, Span data) {
      return data.toByteArray();
    }
  }

  public static class De implements Deserializer<Span> {

    @Override
    public Span deserialize(String topic, byte[] data) {
      try {
        return Span.parseFrom(data);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
