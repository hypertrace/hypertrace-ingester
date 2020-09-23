package org.hypertrace.core.kafkastreams.framework.serdes;

import java.util.Map;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka streams Serde implementation for Avro records and this SerDe does not depend on schema
 * registry service. This implementation injects writer schema of the producer avro record in record
 * headers.
 *
 * WARNING: This can impact performance severely and not meant to be used in production environments.
 * This can used for hypertrace local deployments and tests as well.
 */
public class AvroSerde<T extends SpecificRecordBase> implements Serde<T> {

  final Serde<T> inner;

  public AvroSerde() {
    this.inner = Serdes.serdeFrom(new AvroSerializer<>(), new AvroDeserializer<>());
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
