package org.hypertrace.metrics.generator;

import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OtlpMetricsSerde implements Serde<ResourceMetrics> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OtlpMetricsSerde.class);

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  @Override
  public Serializer<ResourceMetrics> serializer() {
    return new OtlpMetricsSerde.Ser();
  }

  @Override
  public Deserializer<ResourceMetrics> deserializer() {
    return new OtlpMetricsSerde.De();
  }

  public static class Ser implements Serializer<ResourceMetrics> {
    @Override
    public byte[] serialize(String topic, ResourceMetrics data) {
      try {
        return data.toByteArray();
      } catch (Exception e) {
        LOGGER.error("serialization error:", e);
      }
      return null;
    }
  }

  public static class De implements Deserializer<ResourceMetrics> {
    @Override
    public ResourceMetrics deserialize(String topic, byte[] data) {
      try {
        LOGGER.info("deserialize:{}", OtlpMetricsSerde.class.getName());
        return ResourceMetrics.parseFrom(data);
      } catch (InvalidProtocolBufferException e) {
        LOGGER.error("error:", e);
        throw new RuntimeException(e);
      }
    }
  }
}
