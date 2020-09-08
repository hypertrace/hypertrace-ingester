package org.hypertrace.core.kafkastreams.framework.serdes;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class SchemaRegistryBasedAvroSerde<T extends SpecificRecord> implements
    Deserializer<T>,
    Serializer<T> {

  private final Class<T> clazz;
  private transient KafkaAvroSerializer serializer;
  private transient KafkaAvroDeserializer deserializer;

  public SchemaRegistryBasedAvroSerde(Class<T> clazz) {
    this.clazz = clazz;
    this.serializer = new KafkaAvroSerializer();
    this.deserializer = new KafkaAvroDeserializer();
  }

  @Override
  public void configure(Map<String, ?> serdeConfig, boolean isKey) {
    // Required. this triggers the initialization of underlying schema registry client
    this.serializer.configure(serdeConfig, isKey);
    // Required. this triggers the initialization of underlying schema registry client
    this.deserializer.configure(serdeConfig, isKey);
  }

  @Override
  public byte[] serialize(String topic, T data) {
    return serializer.serialize(topic, data);
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    return (T) deserializer.deserialize(topic, data);
  }

  @Override
  public void close() {
  }
}

