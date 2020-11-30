package org.hypertrace.core.kafkastreams.framework.serdes;

import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka streams Serde implementation for Avro records and this SerDe does not depend on schema
 * registry service. This implementation prepends writer schema of the producer avro record to
 * payload itself.
 * <p>
 * WARNING: This can impact performance severely and not meant to be used in production
 * environments. This can used for hypertrace local deployments and tests as well.
 */
public class GenericAvroSerde<T extends GenericRecord> implements Serde<T> {

  private final Serde<T> inner;

  public GenericAvroSerde() {
    this.inner = Serdes.serdeFrom(new GenericAvroSerializer<>(), new GenericAvroDeserializer<>());
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.serializer().configure(configs, isKey);
    inner.deserializer().configure(configs, isKey);
  }

  @Override
  public Serializer<T> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<T> deserializer() {
    return inner.deserializer();
  }
}
