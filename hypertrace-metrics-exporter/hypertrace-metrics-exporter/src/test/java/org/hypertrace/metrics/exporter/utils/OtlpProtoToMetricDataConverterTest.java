package org.hypertrace.metrics.exporter.utils;

import static io.opentelemetry.sdk.metrics.data.AggregationTemporality.DELTA;

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
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OtlpProtoToMetricDataConverterTest {

  @Test
  public void testGuageMetricData() {
    // test for int values
    ResourceMetrics resourceMetrics =
        prepareMetric("int_num_calls", "number of calls", 10, "Gauge");

    List<MetricData> underTestMetricData =
        OtlpProtoToMetricDataConverter.toMetricData(resourceMetrics);

    Assertions.assertEquals(1, underTestMetricData.size());
    Assertions.assertEquals("int_num_calls", underTestMetricData.get(0).getName());
    Assertions.assertEquals(1, underTestMetricData.get(0).getDoubleGaugeData().getPoints().size());
    underTestMetricData
        .get(0)
        .getDoubleGaugeData()
        .getPoints()
        .forEach(
            dpd -> {
              Assertions.assertEquals(10.0, dpd.getValue());
            });

    // test for double values
    ResourceMetrics resourceMetrics1 =
        prepareMetric("double_num_calls", "number of calls", 5.5, "Gauge");

    List<MetricData> underTestMetricData1 =
        underTestMetricData = OtlpProtoToMetricDataConverter.toMetricData(resourceMetrics1);

    Assertions.assertEquals(1, underTestMetricData1.size());
    Assertions.assertEquals("double_num_calls", underTestMetricData1.get(0).getName());
    Assertions.assertEquals(1, underTestMetricData1.get(0).getDoubleGaugeData().getPoints().size());
    underTestMetricData1
        .get(0)
        .getDoubleGaugeData()
        .getPoints()
        .forEach(
            dpd -> {
              Assertions.assertEquals(5.5, dpd.getValue());
            });
  }

  @Test
  public void testSumMetricData() {
    // test for int values
    ResourceMetrics resourceMetrics = prepareMetric("int_sum_calls", "number of calls", 10, "Sum");

    List<MetricData> underTestMetricData =
        OtlpProtoToMetricDataConverter.toMetricData(resourceMetrics);

    Assertions.assertEquals(1, underTestMetricData.size());
    Assertions.assertEquals("int_sum_calls", underTestMetricData.get(0).getName());
    Assertions.assertEquals(1, underTestMetricData.get(0).getDoubleSumData().getPoints().size());
    underTestMetricData
        .get(0)
        .getDoubleSumData()
        .getPoints()
        .forEach(
            dpd -> {
              Assertions.assertEquals(10.0, dpd.getValue());
            });
    Assertions.assertEquals(false, underTestMetricData.get(0).getDoubleSumData().isMonotonic());
    Assertions.assertEquals(
        DELTA, underTestMetricData.get(0).getDoubleSumData().getAggregationTemporality());

    // test for double values
    ResourceMetrics resourceMetrics1 =
        prepareMetric("double_sum_calls", "number of calls", 10.5, "Sum");

    List<MetricData> underTestMetricData1 =
        OtlpProtoToMetricDataConverter.toMetricData(resourceMetrics1);

    Assertions.assertEquals(1, underTestMetricData1.size());
    Assertions.assertEquals("double_sum_calls", underTestMetricData1.get(0).getName());
    Assertions.assertEquals(1, underTestMetricData1.get(0).getDoubleSumData().getPoints().size());
    underTestMetricData1
        .get(0)
        .getDoubleSumData()
        .getPoints()
        .forEach(
            dpd -> {
              Assertions.assertEquals(10.5, dpd.getValue());
            });
    Assertions.assertEquals(false, underTestMetricData1.get(0).getDoubleSumData().isMonotonic());
    Assertions.assertEquals(
        DELTA, underTestMetricData1.get(0).getDoubleSumData().getAggregationTemporality());
  }

  private Resource prepareResource() {
    return Resource.newBuilder()
        .addAttributes(
            io.opentelemetry.proto.common.v1.KeyValue.newBuilder()
                .setKey("Service")
                .setValue(
                    AnyValue.newBuilder().setStringValue("hypertrace-metrics-exporter").build())
                .build())
        .build();
  }

  private NumberDataPoint prepareNumberDataPoint(Number value) {
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

  private Metric prepareGaugeMetric(String metricName, String metricDesc, Number value) {
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

  private Metric prepareSumMetric(String metricName, String metricDesc, Number value) {
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

  private ResourceMetrics prepareMetric(
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
