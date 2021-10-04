package org.hypertrace.metrics.exporter;

import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsConsumer implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsConsumer.class);
  private static final int CONSUMER_POLL_TIMEOUT_MS = 100;

  private static final String KAFKA_CONFIG_KEY = "kafka.config";
  private static final String INPUT_TOPIC_KEY = "input.topic";

  private final KafkaConsumer<byte[], byte[]> consumer;
  private final InMemoryMetricsProducer inMemoryMetricsProducer;
  private final AtomicBoolean running = new AtomicBoolean(false);

  public MetricsConsumer(Config config, InMemoryMetricsProducer inMemoryMetricsProducer) {
    Properties props = new Properties();
    props.putAll(
        mergeProperties(getBaseProperties(), getFlatMapConfig(config.getConfig(KAFKA_CONFIG_KEY))));
    consumer = new KafkaConsumer<byte[], byte[]>(props);
    consumer.subscribe(Collections.singletonList(config.getString(INPUT_TOPIC_KEY)));
    this.inMemoryMetricsProducer = inMemoryMetricsProducer;
  }

  public void run() {
    running.set(true);
    while (running.get()) {
      // check if any message to commit
      if (inMemoryMetricsProducer.isCommitOffset()) {
        // consumer.commitSync();
        inMemoryMetricsProducer.clearCommitOffset();
      }

      // read new data
      List<ResourceMetrics> resourceMetrics = consume();
      if (!resourceMetrics.isEmpty()) {
        inMemoryMetricsProducer.addMetricData(resourceMetrics);
      }
      waitForSec((long) (1000L * 0.1));
    }
  }

  public void stop() {
    running.set(false);
  }

  public List<ResourceMetrics> consume() {
    List<ResourceMetrics> resourceMetrics = new ArrayList<>();

    ConsumerRecords<byte[], byte[]> records =
        consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MS));
    records.forEach(
        record -> {
          try {
            resourceMetrics.add(ResourceMetrics.parseFrom(record.value()));
          } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Invalid record with exception", e);
          }
        });

    return resourceMetrics;
  }

  public void close() {
    consumer.close();
  }

  private Map<String, Object> getBaseProperties() {
    Map<String, Object> baseProperties = new HashMap<>();
    baseProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "hypertrace-metrics-exporter");
    baseProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    baseProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    baseProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    baseProperties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    baseProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return baseProperties;
  }

  private Map<String, Object> getFlatMapConfig(Config config) {
    Map<String, Object> propertiesMap = new HashMap();
    config.entrySet().stream()
        .forEach(
            (entry) -> {
              propertiesMap.put((String) entry.getKey(), config.getString((String) entry.getKey()));
            });
    return propertiesMap;
  }

  private Map<String, Object> mergeProperties(
      Map<String, Object> baseProps, Map<String, Object> props) {
    Objects.requireNonNull(baseProps);
    props.forEach(baseProps::put);
    return baseProps;
  }

  private void waitForSec(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOGGER.debug("waiting for pushing next records were intruppted");
    }
  }
}
