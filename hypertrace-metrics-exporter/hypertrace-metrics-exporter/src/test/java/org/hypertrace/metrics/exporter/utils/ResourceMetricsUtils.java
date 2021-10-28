package org.hypertrace.metrics.exporter.utils;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.resource.v1.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ResourceMetricsUtils {
  private static Resource prepareResource() {
    return Resource.newBuilder()
        .addAttributes(
            io.opentelemetry.proto.common.v1.KeyValue.newBuilder()
                .setKey("Service")
                .setValue(
                    AnyValue.newBuilder().setStringValue("hypertrace-metrics-exporter").build())
                .build())
        .build();
  }

  private static NumberDataPoint prepareNumberDataPoint(Number value) {
    List<KeyValue> attributes =
        toAttributes(
            Map.of(
                "tenant_id", "__default",
                "service_id", "1234",
                "api_id", "4567"));

    NumberDataPoint.Builder numberDataPointBuilder = NumberDataPoint.newBuilder();
    numberDataPointBuilder.addAllAttributes(attributes);
    numberDataPointBuilder.setTimeUnixNano(
        TimeUnit.NANOSECONDS.convert(
            1634119810000L /*2021-10-13:10-10-10 GMT*/, TimeUnit.MILLISECONDS));

    if (value instanceof Integer) {
      numberDataPointBuilder.setAsInt(value.intValue());
    } else {
      numberDataPointBuilder.setAsDouble(value.doubleValue());
    }

    return numberDataPointBuilder.build();
  }

  private static List<io.opentelemetry.proto.common.v1.KeyValue> toAttributes(
      Map<String, String> labels) {
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

  private static Metric prepareGaugeMetric(String metricName, String metricDesc, Number value) {
    io.opentelemetry.proto.metrics.v1.Metric.Builder metricBuilder =
        io.opentelemetry.proto.metrics.v1.Metric.newBuilder();
    metricBuilder.setName(metricName);
    metricBuilder.setDescription(metricDesc);
    metricBuilder.setUnit("1");

    NumberDataPoint numberDataPoint = prepareNumberDataPoint(value);

    Gauge.Builder gaugeBuilder = Gauge.newBuilder();
    gaugeBuilder.addDataPoints(numberDataPoint);
    metricBuilder.setGauge(gaugeBuilder.build());
    return metricBuilder.build();
  }

  private static Metric prepareSumMetric(String metricName, String metricDesc, Number value) {
    io.opentelemetry.proto.metrics.v1.Metric.Builder metricBuilder =
        io.opentelemetry.proto.metrics.v1.Metric.newBuilder();
    metricBuilder.setName(metricName);
    metricBuilder.setDescription(metricDesc);
    metricBuilder.setUnit("1");

    NumberDataPoint numberDataPoint = prepareNumberDataPoint(value);

    Sum.Builder sumBuilder = Sum.newBuilder();
    sumBuilder.addDataPoints(numberDataPoint);
    sumBuilder.setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA);
    sumBuilder.setIsMonotonic(false);
    metricBuilder.setSum(sumBuilder.build());
    return metricBuilder.build();
  }

  public static ResourceMetrics prepareMetric(
      String metricName, String metricDesc, Number value, String type) {

    ResourceMetrics.Builder resourceMetricsBuilder = ResourceMetrics.newBuilder();
    resourceMetricsBuilder.setResource(prepareResource());

    Metric metric;
    if (type.equals("Gauge")) {
      metric = prepareGaugeMetric(metricName, metricDesc, value);
    } else {
      metric = prepareSumMetric(metricName, metricDesc, value);
    }

    resourceMetricsBuilder.addInstrumentationLibraryMetrics(
        InstrumentationLibraryMetrics.newBuilder()
            .addMetrics(metric)
            .setInstrumentationLibrary(
                InstrumentationLibrary.newBuilder().setName("Generated").build())
            .build());

    return resourceMetricsBuilder.build();
  }
}
