package org.hypertrace.metrics.generator;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.viewgenerator.api.RawServiceView;

public class MetricsExtractor
    implements Transformer<String, RawServiceView, KeyValue<byte[], List<ResourceMetrics>>> {

  LinkedHashMap<MetricIdentity, Long> metricIdentityMap = new LinkedHashMap<>();

  @Override
  public void init(ProcessorContext context) {
    metricIdentityMap = new LinkedHashMap<>();
  }

  @Override
  public KeyValue<byte[], List<ResourceMetrics>> transform(String key, RawServiceView value) {

    // create a metric
    Map<String, String> attributes = new HashMap<>();
    attributes.put("tenant_id", value.getTenantId());
    attributes.put("consumer_id", "1");
    attributes.put("service_id", value.getServiceId());
    attributes.put("service_name", value.getServiceName());
    attributes.put("api_id", value.getApiId());
    attributes.put("api_name", value.getApiName());

    Metric metric = new Metric("num_calls", attributes);

    // create metric identity (timestamp, key, metric)
    Instant instant =
        Instant.ofEpochMilli(value.getStartTimeMillis())
            .plusSeconds(1)
            .truncatedTo(ChronoUnit.SECONDS);
    MetricIdentity metricsIdentity =
        new MetricIdentity(instant.toEpochMilli(), metric.getKey(), metric);

    // update the cache
    long preValue = 0;
    if (metricIdentityMap.containsKey(metricsIdentity)) {
      preValue = metricIdentityMap.get(metricsIdentity);
    }
    metricIdentityMap.put(metricsIdentity, preValue + 1);

    // go over all the map values, that are older than 15 secs
    Instant fifteenSecsPrior = Instant.now().minus(15, ChronoUnit.SECONDS);
    List<Map.Entry<MetricIdentity, Long>> metricIdentities =
        metricIdentityMap.entrySet().stream()
            .filter(k -> k.getKey().getTimeStampSecs() <= fifteenSecsPrior.toEpochMilli())
            .collect(Collectors.toList());

    List<ResourceMetrics> resourceMetrics =
        metricIdentities.stream()
            .map(k -> convertToResourceMetric(k.getKey(), k.getValue()))
            .collect(Collectors.toList());

    // remove the keys from map
    metricIdentities.stream().forEach(k -> metricIdentityMap.remove(k.getKey()));

    if (resourceMetrics.size() > 0) {
      return new KeyValue<>(null, resourceMetrics);
    }

    return null;
  }

  private ResourceMetrics convertToResourceMetric(MetricIdentity metricIdentity, Long value) {
    ResourceMetrics.Builder resourceMetricsBuilder = ResourceMetrics.newBuilder();
    resourceMetricsBuilder.setResource(
        Resource.newBuilder()
            .addAttributes(
                io.opentelemetry.proto.common.v1.KeyValue.newBuilder()
                    .setKey("Service")
                    .setValue(AnyValue.newBuilder().setStringValue("metrics-extractor").build())
                    .build()));

    io.opentelemetry.proto.metrics.v1.Metric.Builder metricBuilder =
        io.opentelemetry.proto.metrics.v1.Metric.newBuilder();
    metricBuilder.setName(metricIdentity.getMetric().getName());
    metricBuilder.setDescription("number of calls");
    metricBuilder.setUnit("1");

    NumberDataPoint.Builder numberDataPointBuilder = NumberDataPoint.newBuilder();
    List<io.opentelemetry.proto.common.v1.KeyValue> attributes =
        toAttributes(metricIdentity.getMetric().getAttributes());
    numberDataPointBuilder.addAllAttributes(attributes);
    numberDataPointBuilder.setTimeUnixNano(
        TimeUnit.NANOSECONDS.convert(metricIdentity.getTimeStampSecs(), TimeUnit.MILLISECONDS));
    numberDataPointBuilder.setAsInt(value);

    Gauge.Builder gaugeBuilder = Gauge.newBuilder();
    gaugeBuilder.addDataPoints(numberDataPointBuilder.build());
    metricBuilder.setGauge(gaugeBuilder.build());

    resourceMetricsBuilder.addInstrumentationLibraryMetrics(
        InstrumentationLibraryMetrics.newBuilder()
            .addMetrics(metricBuilder.build())
            .setInstrumentationLibrary(
                InstrumentationLibrary.newBuilder().setName("Generated").build())
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
                  keyValueBuilder.setValue(
                      AnyValue.newBuilder()
                          .setStringValue(value)
                          .build()
                  );
                  return keyValueBuilder.build();
                })
            .collect(Collectors.toList());
    return attributes;
  }

  @Override
  public void close() {}
}
