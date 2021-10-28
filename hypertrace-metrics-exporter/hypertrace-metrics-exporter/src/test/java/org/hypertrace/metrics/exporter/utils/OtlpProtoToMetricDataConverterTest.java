package org.hypertrace.metrics.exporter.utils;

import static io.opentelemetry.sdk.metrics.data.AggregationTemporality.DELTA;
import static org.hypertrace.metrics.exporter.utils.ResourceMetricsUtils.prepareMetric;

import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.List;
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
}
