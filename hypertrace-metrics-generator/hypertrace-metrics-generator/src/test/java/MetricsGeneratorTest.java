import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.metrics.generator.MetricsGenerator;
import org.hypertrace.metrics.generator.OtlpMetricsSerde;
import org.hypertrace.viewgenerator.api.RawServiceView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class MetricsGeneratorTest {
  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "hypertrace-metrics-generator")
  public void testMetricsGenerator(@TempDir Path tempDir) {

    MetricsGenerator underTest = new MetricsGenerator(ConfigClientFactory.getClient());
    Config config =
        ConfigFactory.parseURL(
            getClass()
                .getClassLoader()
                .getResource("configs/hypertrace-metrics-generator/application.conf"));

    Map<String, Object> baseProps = underTest.getBaseStreamsConfig();
    Map<String, Object> streamsProps = underTest.getStreamsConfig(config);
    baseProps.forEach(streamsProps::put);
    Map<String, Object> mergedProps = streamsProps;

    File file = tempDir.resolve("state").toFile();
    mergedProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    mergedProps.put(MetricsGenerator.METRICS_GENERATOR_JOB_CONFIG, config);
    mergedProps.put(StreamsConfig.STATE_DIR_CONFIG, file.getAbsolutePath());

    StreamsBuilder streamsBuilder =
        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Properties props = new Properties();
    mergedProps.forEach(props::put);

    Serde defaultValueSerde = new StreamsConfig(mergedProps).defaultValueSerde();

    TopologyTestDriver td = new TopologyTestDriver(streamsBuilder.build(), props);
    TestInputTopic<String, RawServiceView> inputTopic =
        td.createInputTopic(
            config.getString(MetricsGenerator.INPUT_TOPIC_CONFIG_KEY),
            Serdes.String().serializer(),
            defaultValueSerde.serializer());

    TestOutputTopic outputTopic =
        td.createOutputTopic(
            config.getString(MetricsGenerator.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().deserializer(),
            new OtlpMetricsSerde().deserializer());

    String tenantId = "tenant1";

    // create 3 rows within 30 secs interval
    RawServiceView row1 =
        RawServiceView.newBuilder()
            .setTenantId(tenantId)
            .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
            .setSpanId(ByteBuffer.wrap("span-1".getBytes()))
            .setApiId("api-1234")
            .setApiName("GET /api/v1/name")
            .setServiceId("svc-1234")
            .setServiceName("Cart Service")
            .setStartTimeMillis(1636982920000L)
            .setEndTimeMillis(1636982920015L)
            .setDurationMillis(15L)
            .build();

    RawServiceView row2 =
        RawServiceView.newBuilder()
            .setTenantId(tenantId)
            .setTraceId(ByteBuffer.wrap("trace-2".getBytes()))
            .setSpanId(ByteBuffer.wrap("span-2".getBytes()))
            .setApiId("api-1234")
            .setApiName("GET /api/v1/name")
            .setServiceId("svc-1234")
            .setServiceName("Cart Service")
            .setStartTimeMillis(1636982920200L)
            .setEndTimeMillis(1636982920215L)
            .setDurationMillis(15L)
            .build();

    RawServiceView row3 =
        RawServiceView.newBuilder()
            .setTenantId(tenantId)
            .setTraceId(ByteBuffer.wrap("trace-3".getBytes()))
            .setSpanId(ByteBuffer.wrap("span-3".getBytes()))
            .setApiId("api-1234")
            .setApiName("GET /api/v1/name")
            .setServiceId("svc-1234")
            .setServiceName("Cart Service")
            .setStartTimeMillis(1636982920400L)
            .setEndTimeMillis(1636982920415L)
            .setDurationMillis(15L)
            .build();

    // pipe the data
    inputTopic.pipeInput(null, row1);
    inputTopic.pipeInput(null, row2);
    inputTopic.pipeInput(null, row3);

    // advance clock < 30s
    td.advanceWallClockTime(Duration.ofMillis(200));
    assertTrue(outputTopic.isEmpty());

    // advance to > 30s
    td.advanceWallClockTime(Duration.ofSeconds(32));
    ResourceMetrics resourceMetrics = (ResourceMetrics) outputTopic.readValue();
    assertNotNull(resourceMetrics);

    // expect the num_call count is 3
  }
}
