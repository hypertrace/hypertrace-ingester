package org.hypertrace.metrics.exporter.utils;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoubleGaugeData;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.DoubleSumData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OtlpProtoToMetricDataConverter {

  private static Resource toResource(io.opentelemetry.proto.resource.v1.Resource otlpResource) {
    return Resource.create(toAttributes(otlpResource.getAttributesList()));
  }

  private static InstrumentationLibraryInfo toInstrumentationLibraryInfo(
      InstrumentationLibrary otlpInstrumentationLibraryInfo) {
    return InstrumentationLibraryInfo.create(
        otlpInstrumentationLibraryInfo.getName(), otlpInstrumentationLibraryInfo.getVersion());
  }

  private static Attributes toAttributes(List<KeyValue> keyValues) {
    AttributesBuilder attributesBuilder = Attributes.builder();
    keyValues.forEach(
        keyValue -> {
          attributesBuilder.put(keyValue.getKey(), keyValue.getValue().getStringValue());
        });
    return attributesBuilder.build();
  }

  private static List<DoublePointData> toDoublePointData(List<NumberDataPoint> numberDataPoints) {
    return numberDataPoints.stream()
        .map(
            numberDataPoint ->
                DoublePointData.create(
                    numberDataPoint.getStartTimeUnixNano(),
                    numberDataPoint.getTimeUnixNano(),
                    toAttributes(numberDataPoint.getAttributesList()),
                    numberDataPoint.getAsInt()))
        .collect(Collectors.toList());
  }

  private static AggregationTemporality toAggregationTemporality(
      io.opentelemetry.proto.metrics.v1.AggregationTemporality aggregationTemporality) {
    switch (aggregationTemporality) {
      case AGGREGATION_TEMPORALITY_CUMULATIVE:
        return AggregationTemporality.CUMULATIVE;
      case AGGREGATION_TEMPORALITY_DELTA:
        return AggregationTemporality.DELTA;
      default:
        return AggregationTemporality.CUMULATIVE;
    }
  }

  private static MetricData toGaugeMetricData(
      Resource resource, InstrumentationLibraryInfo instrumentationLibraryInfo, Metric metric) {
    Gauge gaugeMetric = metric.getGauge();

    DoubleGaugeData data =
        DoubleGaugeData.create(toDoublePointData(gaugeMetric.getDataPointsList()));

    return MetricData.createDoubleGauge(
        resource,
        instrumentationLibraryInfo,
        metric.getName(),
        metric.getDescription(),
        metric.getUnit(),
        data);
  }

  private static MetricData toSumMetricData(
      Resource resource, InstrumentationLibraryInfo instrumentationLibraryInfo, Metric metric) {
    Sum sumMetric = metric.getSum();

    DoubleSumData doubleSumData =
        DoubleSumData.create(
            sumMetric.getIsMonotonic(),
            toAggregationTemporality(sumMetric.getAggregationTemporality()),
            toDoublePointData(sumMetric.getDataPointsList()));

    return MetricData.createDoubleSum(
        resource,
        instrumentationLibraryInfo,
        metric.getName(),
        metric.getDescription(),
        metric.getUnit(),
        doubleSumData);
  }

  private static MetricData toMetricData(
      Resource resource, InstrumentationLibraryInfo instrumentationLibraryInfo, Metric metric) {
    switch (metric.getDataCase()) {
      case GAUGE:
        return toGaugeMetricData(resource, instrumentationLibraryInfo, metric);
      case SUM:
        return toSumMetricData(resource, instrumentationLibraryInfo, metric);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported metric type: %s", metric.getDataCase()));
    }
  }

  public static List<MetricData> toMetricData(ResourceMetrics resourceMetrics) {
    List<MetricData> metricData = new ArrayList<>();
    Resource resource = toResource(resourceMetrics.getResource());
    resourceMetrics
        .getInstrumentationLibraryMetricsList()
        .forEach(
            instrumentationLibraryMetrics -> {
              InstrumentationLibraryInfo instrumentationLibraryInfo =
                  toInstrumentationLibraryInfo(
                      instrumentationLibraryMetrics.getInstrumentationLibrary());
              instrumentationLibraryMetrics
                  .getMetricsList()
                  .forEach(metric -> toMetricData(resource, instrumentationLibraryInfo, metric));
            });
    return metricData;
  }
}
