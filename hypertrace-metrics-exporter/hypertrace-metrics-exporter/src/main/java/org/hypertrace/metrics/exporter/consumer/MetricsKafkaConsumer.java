package org.hypertrace.metrics.exporter.consumer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.hypertrace.metrics.exporter.producer.InMemoryMetricsProducer;
import org.hypertrace.metrics.exporter.utils.OtlpProtoToMetricDataConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsKafkaConsumer implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsKafkaConsumer.class);

  private static final int CONSUMER_POLL_TIMEOUT_MS = 100;
  private static final int QUEUE_WAIT_TIME_MS = 500;
  private static final int WAIT_TIME_MS = 100;

  private static final String KAFKA_CONFIG_KEY = "kafka.config";
  private static final String INPUT_TOPIC_KEY = "input.topic";

  private final KafkaConsumer<byte[], byte[]> consumer;
  private final InMemoryMetricsProducer inMemoryMetricsProducer;

  public MetricsKafkaConsumer(Config config, InMemoryMetricsProducer inMemoryMetricsProducer) {
    consumer = new KafkaConsumer<>(prepareProperties(config.getConfig(KAFKA_CONFIG_KEY)));
    consumer.subscribe(Collections.singletonList(config.getString(INPUT_TOPIC_KEY)));
    this.inMemoryMetricsProducer = inMemoryMetricsProducer;
  }

  public void run() {
    while (true && !Thread.currentThread().isInterrupted()) {
      List<ResourceMetrics> resourceMetrics = new ArrayList<>();

      ConsumerRecords<byte[], byte[]> records =
          consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MS));

      records.forEach(
          record -> {
            try {
              resourceMetrics.add(ResourceMetrics.parseFrom(record.value()));
            } catch (InvalidProtocolBufferException e) {
              LOGGER.warn("Skipping record due to error", e);
            }
          });

      resourceMetrics.forEach(
          rm -> {
            try {
              List<MetricData> metricData = OtlpProtoToMetricDataConverter.toMetricData(rm);
              boolean result = false;
              while (!result) {
                result = inMemoryMetricsProducer.addMetricData(metricData);
                waitForSec(QUEUE_WAIT_TIME_MS);
              }
            } catch (Exception e) {
              LOGGER.debug("skipping the resource metrics due to error: {}", e);
            }
          });

      waitForSec(WAIT_TIME_MS);
    }
  }

  public void close() {
    consumer.close();
  }

  private Properties prepareProperties(Config config) {
    Map<String, Object> overrideProperties = new HashMap();
    config.entrySet().stream()
        .forEach(e -> overrideProperties.put(e.getKey(), config.getString(e.getKey())));

    Map<String, Object> baseProperties = getBaseProperties();
    overrideProperties.forEach(baseProperties::put);

    Properties properties = new Properties();
    properties.putAll(baseProperties);
    return properties;
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

  private void waitForSec(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.debug("While waiting, the consumer thread has interrupted");
    }
  }
}
