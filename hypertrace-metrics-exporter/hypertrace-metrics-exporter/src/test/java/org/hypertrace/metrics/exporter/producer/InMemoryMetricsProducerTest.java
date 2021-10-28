package org.hypertrace.metrics.exporter.producer;

import static org.hypertrace.metrics.exporter.utils.ResourceMetricsUtils.prepareMetric;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.List;
import org.hypertrace.metrics.exporter.utils.OtlpProtoToMetricDataConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InMemoryMetricsProducerTest {

  private InMemoryMetricsProducer underTest;

  @BeforeEach
  public void setUp() {
    Config config =
        ConfigFactory.parseURL(
            getClass()
                .getClassLoader()
                .getResource("configs/hypertrace-metrics-exporter/application.conf"));

    underTest = new InMemoryMetricsProducer(config);
  }

  @Test
  public void testAddMetricDataAndCollectData() {
    // insert 1 data
    ResourceMetrics resourceMetrics = prepareMetric("int_num_calls", "number of calls", 1, "Gauge");
    List<MetricData> inMetricData = OtlpProtoToMetricDataConverter.toMetricData(resourceMetrics);

    Assertions.assertTrue(underTest.addMetricData(inMetricData));

    // insert 2nd data
    ResourceMetrics resourceMetrics1 =
        prepareMetric("double_num_calls", "number of calls", 2.5, "Gauge");
    List<MetricData> inMetricData1 = OtlpProtoToMetricDataConverter.toMetricData(resourceMetrics1);

    // assert that can't add
    Assertions.assertTrue(underTest.addMetricData(inMetricData1));

    // insert 3nd data
    ResourceMetrics resourceMetrics2 =
        prepareMetric("double_num_calls", "number of calls", 3.5, "Gauge");
    List<MetricData> inMetricData2 = OtlpProtoToMetricDataConverter.toMetricData(resourceMetrics2);

    // assert that can't add
    Assertions.assertFalse(underTest.addMetricData(inMetricData2));

    // Now read data
    List<MetricData> outData = (List) underTest.collectAllMetrics();
    Assertions.assertEquals(1, outData.size());
    Assertions.assertEquals(inMetricData.get(0), outData.get(0));

    outData = (List) underTest.collectAllMetrics();
    Assertions.assertEquals(1, outData.size());
    Assertions.assertEquals(inMetricData1.get(0), outData.get(0));

    outData = (List) underTest.collectAllMetrics();
    Assertions.assertEquals(0, outData.size());

    // reinsert 3rd data point
    Assertions.assertTrue(underTest.addMetricData(inMetricData2));
    outData = (List) underTest.collectAllMetrics();
    Assertions.assertEquals(1, outData.size());
    Assertions.assertEquals(inMetricData2.get(0), outData.get(0));
  }
}
