package org.hypertrace.metrics.generator.api.v1.serde;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.hypertrace.metrics.generator.api.v1.Metric;

public class MetricSerde implements Serde<Metric> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  @Override
  public Serializer<Metric> serializer() {
    return new MetricSerde.MetricSerializer();
  }

  @Override
  public Deserializer<Metric> deserializer() {
    return new MetricSerde.MetricDeserializer();
  }

  private static class MetricSerializer implements Serializer<Metric> {
    @Override
    public byte[] serialize(String topic, Metric data) {
      try {
        return data.toByteArray();
      } catch (Exception e) {
        // ignore error
      }
      return null;
    }
  }

  private static class MetricDeserializer implements Deserializer<Metric> {
    @Override
    public Metric deserialize(String topic, byte[] data) {
      try {
        return Metric.parseFrom(data);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
