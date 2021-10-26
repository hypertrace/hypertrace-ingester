package org.hypertrace.metrics.processor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class MetricsProcessorTest {
  private MetricsProcessor underTest;
  private Config underTestConfig;

  @BeforeEach
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "hypertrace-metrics-processor")
  public void setUp() {
    underTest = new MetricsProcessor(ConfigClientFactory.getClient());
    underTestConfig =
        ConfigFactory.parseURL(
                getClass()
                    .getClassLoader()
                    .getResource("configs/hypertrace-metrics-processor/application.conf"))
            .resolve();
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "hypertrace-metrics-processor")
  public void testMetricsProcessorTopology() {
    // prepare stream properties
    Map<String, Object> mergedProps = new HashMap<>();
    underTest.getBaseStreamsConfig().forEach(mergedProps::put);
    underTest.getStreamsConfig(underTestConfig).forEach(mergedProps::put);
    mergedProps.put(underTest.getJobConfigKey(), underTestConfig);

    StreamsBuilder streamsBuilder =
        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Properties props = new Properties();
    mergedProps.forEach(props::put);

    // create topology test driver, and i/o topics
    TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), props);

    TestInputTopic<byte[], ResourceMetrics> inputTopic =
        topologyTestDriver.createInputTopic(
            underTestConfig.getString(MetricsProcessor.INPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().serializer(),
            new OtlpMetricsSerde().serializer());

    TestOutputTopic outputTopic =
        topologyTestDriver.createOutputTopic(
            underTestConfig.getString(MetricsProcessor.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().deserializer(),
            new OtlpMetricsSerde().deserializer());

    // create resource metrics and add into pipeline
    ResourceMetrics inputResourceMetrics = getTestMetricsGauge("num_calls", "number of calls", 20L);
    inputTopic.pipeInput(inputResourceMetrics);
    ResourceMetrics outputResourceMetrics = (ResourceMetrics) outputTopic.readValue();

    // verification
    Assertions.assertNotNull(outputResourceMetrics);
    Assertions.assertNotNull(outputResourceMetrics.getResource());

    Assertions.assertEquals(1, outputResourceMetrics.getInstrumentationLibraryMetricsCount());
    Assertions.assertEquals(
        1, outputResourceMetrics.getInstrumentationLibraryMetrics(0).getMetricsCount());

    Assertions.assertEquals(
        "num_calls",
        outputResourceMetrics.getInstrumentationLibraryMetrics(0).getMetrics(0).getName());

    // data points verification
    Gauge outGauge =
        outputResourceMetrics.getInstrumentationLibraryMetrics(0).getMetrics(0).getGauge();
    Assertions.assertNotNull(outGauge);
    Assertions.assertEquals(1, outGauge.getDataPointsCount());
    Assertions.assertEquals(
        1634119810000L,
        TimeUnit.MILLISECONDS.convert(
            outGauge.getDataPoints(0).getTimeUnixNano(), TimeUnit.NANOSECONDS));
    Assertions.assertEquals(3, outGauge.getDataPoints(0).getAttributesCount());
    Assertions.assertEquals(20L, outGauge.getDataPoints(0).getAsInt());
  }

  private ResourceMetrics getTestMetricsGauge(String metricName, String metricDesc, Long value) {
    ResourceMetrics.Builder resourceMetricsBuilder = ResourceMetrics.newBuilder();
    resourceMetricsBuilder.setResource(
        Resource.newBuilder()
            .addAttributes(
                io.opentelemetry.proto.common.v1.KeyValue.newBuilder()
                    .setKey("Service")
                    .setValue(
                        AnyValue.newBuilder()
                            .setStringValue("hypertrace-metrics-processor")
                            .build())
                    .build()));

    io.opentelemetry.proto.metrics.v1.Metric.Builder metricBuilder =
        io.opentelemetry.proto.metrics.v1.Metric.newBuilder();
    metricBuilder.setName(metricName);
    metricBuilder.setDescription(metricDesc);
    metricBuilder.setUnit("1");

    NumberDataPoint.Builder numberDataPointBuilder = NumberDataPoint.newBuilder();
    List<KeyValue> attributes =
        toAttributes(
            Map.of(
                "tenant_id", "__default",
                "service_id", "1234",
                "api_id", "4567"));
    numberDataPointBuilder.addAllAttributes(attributes);
    numberDataPointBuilder.setTimeUnixNano(
        TimeUnit.NANOSECONDS.convert(
            1634119810000L /*2021-10-13:10-10-10 GMT*/, TimeUnit.MILLISECONDS));
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
                  keyValueBuilder.setValue(AnyValue.newBuilder().setStringValue(value).build());
                  return keyValueBuilder.build();
                })
            .collect(Collectors.toList());
    return attributes;
  }
}
