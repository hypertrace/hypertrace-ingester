package org.hypertrace.metrics.generator;

import static java.util.stream.Collectors.joining;
import static org.hypertrace.metrics.generator.MetricsGenerator.OUTPUT_TOPIC_PRODUCER;

import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hypertrace.metrics.generator.api.Metric;
import org.hypertrace.metrics.generator.api.MetricIdentity;
import org.hypertrace.viewgenerator.api.RawServiceView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsProcessor
    implements Transformer<String, RawServiceView, KeyValue<byte[], ResourceMetrics>> {

  private static final Logger logger = LoggerFactory.getLogger(MetricsProcessor.class);

  private static final String TENANT_ID_ATTR = "tenant_id";
  private static final String SERVICE_ID_ATTR = "service_id";
  private static final String SERVICE_NAME_ATTR = "service_name";
  private static final String API_ID = "api_id";
  private static final String API_NAME = "api_name";
  private static final String CONSUMER_ID_ATTR = "consumer_id";
  private static final String METRIC_NUM_CALLS = "num_calls";
  private static final String METRIC_NUM_CALLS_DESCRIPTION = "num of calls";
  private static final String METRIC_NUM_CALLS_UNIT = "1";
  private static final String DELIMITER = ":";

  private ProcessorContext context;
  private KeyValueStore<MetricIdentity, Long> metricsIdentityStore;
  private KeyValueStore<MetricIdentity, Metric> metricsStore;
  private long groupingWindowTimeoutMs = 30000L;
  private To outputTopicProducer;

  @Override
  public void init(ProcessorContext context) {

    this.context = context;
    this.metricsIdentityStore =
        (KeyValueStore<MetricIdentity, Long>)
            context.getStateStore(MetricsGenerator.METRICS_IDENTITY_STORE);
    this.metricsStore =
        (KeyValueStore<MetricIdentity, Metric>)
            context.getStateStore(MetricsGenerator.METRICS_IDENTITY_VALUE_STORE);
    this.outputTopicProducer = To.child(OUTPUT_TOPIC_PRODUCER);
    restorePunctuators();
  }

  @Override
  public KeyValue<byte[], ResourceMetrics> transform(String key, RawServiceView value) {

    // create a metricX
    Map<String, String> attributes = new HashMap<>();
    attributes.put(TENANT_ID_ATTR, value.getTenantId());
    attributes.put(CONSUMER_ID_ATTR, "1");
    attributes.put(SERVICE_ID_ATTR, value.getServiceId());
    attributes.put(SERVICE_NAME_ATTR, value.getServiceName());
    attributes.put(API_ID, value.getApiId());
    attributes.put(API_NAME, value.getApiName());

    Metric metric =
        Metric.newBuilder()
            .setName(METRIC_NUM_CALLS)
            .setDescription(METRIC_NUM_CALLS_DESCRIPTION)
            .setUnit(METRIC_NUM_CALLS_UNIT)
            .setAttributes(attributes)
            .build();

    // create metricX identity (timestamp, metric_key)
    Instant instant =
        Instant.ofEpochMilli(value.getStartTimeMillis())
            .plusSeconds(1)
            .truncatedTo(ChronoUnit.SECONDS);

    MetricIdentity metricsIdentity =
        MetricIdentity.newBuilder()
            .setTimestampMillis(instant.toEpochMilli())
            .setMetricKey(generateKey(metric))
            .build();

    // update the cache
    Long preValue = metricsIdentityStore.get(metricsIdentity);
    if (preValue == null) {
      // first entry
      metricsIdentityStore.put(metricsIdentity, 1L);
      metricsStore.put(metricsIdentity, metric);

      // schedule a punctuator
      schedulePunctuator(metricsIdentity);
    } else {
      metricsIdentityStore.put(metricsIdentity, preValue + 1);
    }

    return null;
  }

  @Override
  public void close() {}

  private String generateKey(Metric metric) {
    String attributesStr =
        metric.getAttributes().entrySet().stream()
            .map(Object::toString)
            .collect(joining(DELIMITER));
    String id = String.join(DELIMITER, metric.getName(), attributesStr);
    return UUID.nameUUIDFromBytes(id.getBytes()).toString();
  }

  private void restorePunctuators() {
    long count = 0;
    Instant start = Instant.now();
    try (KeyValueIterator<MetricIdentity, Long> it = metricsIdentityStore.all()) {
      while (it.hasNext()) {
        schedulePunctuator(it.next().key);
        count++;
      }
      logger.info(
          "Restored=[{}] punctuators, Duration=[{}]",
          count,
          Duration.between(start, Instant.now()));
    }
  }

  private void schedulePunctuator(MetricIdentity key) {
    MetricEmitPunctuator punctuator =
        new MetricEmitPunctuator(
            key,
            context,
            metricsIdentityStore,
            metricsStore,
            groupingWindowTimeoutMs,
            outputTopicProducer);
    Cancellable cancellable =
        context.schedule(
            Duration.ofMillis(groupingWindowTimeoutMs),
            PunctuationType.WALL_CLOCK_TIME,
            punctuator);
    punctuator.setCancellable(cancellable);
    logger.debug(
        "Scheduled a punctuator to emit trace for key=[{}] to run after [{}] ms",
        key,
        groupingWindowTimeoutMs);
  }
}
