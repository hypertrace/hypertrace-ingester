package org.hypertrace.metrics.generator.api.v1.serde;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.hypertrace.metrics.generator.api.v1.MetricIdentity;

public class MetricIdentitySerde implements Serde<MetricIdentity> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  @Override
  public Serializer<MetricIdentity> serializer() {
    return new MetricIdentitySerde.MetricIdentitySerializer();
  }

  @Override
  public Deserializer<MetricIdentity> deserializer() {
    return new MetricIdentitySerde.MetricIdentityDeserializer();
  }

  private static class MetricIdentitySerializer implements Serializer<MetricIdentity> {
    @Override
    public byte[] serialize(String topic, MetricIdentity data) {
      try {
        return data.toByteArray();
      } catch (Exception e) {
        // ignore error
      }
      return null;
    }
  }

  private static class MetricIdentityDeserializer implements Deserializer<MetricIdentity> {
    @Override
    public MetricIdentity deserialize(String topic, byte[] data) {
      try {
        return MetricIdentity.parseFrom(data);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
