package org.hypertrace.metrics.generator;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hypertrace.metrics.generator.api.v1.Metric;
import org.hypertrace.metrics.generator.api.v1.MetricIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricEmitPunctuator implements Punctuator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricEmitPunctuator.class);

  private static final String RESOURCE_KEY_SERVICE = "service";
  private static final String RESOURCE_KEY_SERVICE_VALUE = "metrics-generator";
  private static final String INSTRUMENTATION_LIB_NAME = "Generated-From-View";

  private MetricIdentity key;
  private ProcessorContext context;
  private KeyValueStore<MetricIdentity, Long> metricIdentityStore;
  private KeyValueStore<MetricIdentity, Metric> metricStore;
  private Cancellable cancellable;
  private To outputTopicProducer;

  public MetricEmitPunctuator(
      MetricIdentity key,
      ProcessorContext context,
      KeyValueStore<MetricIdentity, Long> metricIdentityStore,
      KeyValueStore<MetricIdentity, Metric> metricStore,
      To outputTopicProducer) {
    this.key = key;
    this.context = context;
    this.metricIdentityStore = metricIdentityStore;
    this.metricStore = metricStore;
    this.outputTopicProducer = outputTopicProducer;
  }

  public void setCancellable(Cancellable cancellable) {
    this.cancellable = cancellable;
  }

  @Override
  public void punctuate(long timestamp) {
    // always cancel the punctuator else it will get re-scheduled automatically
    cancellable.cancel();

    // read the value from a key
    Long value = metricIdentityStore.get(this.key);
    if (value != null) {
      long diff = timestamp - this.key.getTimestampMillis();
      LOGGER.debug("Metrics with key:{} is emitted after duration {}", this.key, diff);

      Metric metric = metricStore.get(this.key);
      metricIdentityStore.delete(this.key);
      metricStore.delete(this.key);
      // convert to Resource Metrics
      ResourceMetrics resourceMetrics = convertToResourceMetric(this.key, value, metric);
      context.forward(null, resourceMetrics, outputTopicProducer);
    } else {
      LOGGER.debug("The value for metrics with key:{} is null", this.key);
    }
  }

  private ResourceMetrics convertToResourceMetric(
      MetricIdentity metricIdentity, Long value, Metric metric) {
    ResourceMetrics.Builder resourceMetricsBuilder = ResourceMetrics.newBuilder();
    resourceMetricsBuilder.setResource(
        Resource.newBuilder()
            .addAttributes(
                io.opentelemetry.proto.common.v1.KeyValue.newBuilder()
                    .setKey(RESOURCE_KEY_SERVICE)
                    .setValue(
                        AnyValue.newBuilder().setStringValue(RESOURCE_KEY_SERVICE_VALUE).build())
                    .build()));

    io.opentelemetry.proto.metrics.v1.Metric.Builder metricBuilder =
        io.opentelemetry.proto.metrics.v1.Metric.newBuilder();
    metricBuilder.setName(metric.getName());
    metricBuilder.setDescription(metric.getDescription());
    metricBuilder.setUnit(metric.getUnit());

    NumberDataPoint.Builder numberDataPointBuilder = NumberDataPoint.newBuilder();
    List<KeyValue> attributes = toAttributes(metric.getAttributes());
    numberDataPointBuilder.addAllAttributes(attributes);
    numberDataPointBuilder.setTimeUnixNano(
        TimeUnit.NANOSECONDS.convert(metricIdentity.getTimestampMillis(), TimeUnit.MILLISECONDS));
    numberDataPointBuilder.setAsInt(value);

    Gauge.Builder gaugeBuilder = Gauge.newBuilder();
    gaugeBuilder.addDataPoints(numberDataPointBuilder.build());
    metricBuilder.setGauge(gaugeBuilder.build());

    resourceMetricsBuilder.addInstrumentationLibraryMetrics(
        InstrumentationLibraryMetrics.newBuilder()
            .addMetrics(metricBuilder.build())
            .setInstrumentationLibrary(
                InstrumentationLibrary.newBuilder().setName(INSTRUMENTATION_LIB_NAME).build())
            .build());

    return resourceMetricsBuilder.build();
  }

  private List<io.opentelemetry.proto.common.v1.KeyValue> toAttributes(Map<String, String> labels) {
    List<io.opentelemetry.proto.common.v1.KeyValue> attributes =
        labels.entrySet().stream()
            .map(
                k -> {
                  io.opentelemetry.proto.common.v1.KeyValue.Builder keyValueBuilder =
                      io.opentelemetry.proto.common.v1.KeyValue.newBuilder();
                  keyValueBuilder.setKey(k.getKey());
                  String value = k.getValue() != null ? k.getValue() : "";
                  keyValueBuilder.setValue(AnyValue.newBuilder().setStringValue(value).build());
                  return keyValueBuilder.build();
                })
            .collect(Collectors.toList());
    return attributes;
  }
}
