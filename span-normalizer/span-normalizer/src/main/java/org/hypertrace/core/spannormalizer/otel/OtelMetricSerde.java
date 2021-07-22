package org.hypertrace.core.spannormalizer.otel;

import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class OtelMetricSerde implements Serde<ResourceMetrics> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  @Override
  public Serializer<ResourceMetrics> serializer() {
    return new OtelMetricSerde.Ser();
  }

  @Override
  public Deserializer<ResourceMetrics> deserializer() {
    return new OtelMetricSerde.De();
  }

  public static class Ser implements Serializer<ResourceMetrics> {

    @Override
    public byte[] serialize(String topic, ResourceMetrics data) {
      return data.toByteArray();
    }
  }

  public static class De implements Deserializer<ResourceMetrics> {

    @Override
    public ResourceMetrics deserialize(String topic, byte[] data) {
      try {
        return ResourceMetrics.parseFrom(data);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
