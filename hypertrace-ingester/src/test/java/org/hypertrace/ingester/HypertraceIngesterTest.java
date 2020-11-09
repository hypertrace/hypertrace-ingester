package org.hypertrace.ingester;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.protobuf.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanSerde;
import org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

/**
 * Topology Test for {@link HypertraceIngester}
 */
public class HypertraceIngesterTest {

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "hypertrace-ingester")
  public void testPacketFlowForEachHop(@TempDir Path tempDir) {
    File file = tempDir.resolve("state").toFile();

    HypertraceIngester underTest = new HypertraceIngester(ConfigClientFactory.getClient());
    Config config = ConfigFactory.parseURL(
        getClass().getClassLoader().getResource("configs/hypertrace-ingester/application.conf"));

    Map<String, Object> baseProps = underTest.getBaseStreamsConfig();
    Map<String, Object> streamsProps = underTest.getStreamsConfig(config);
    streamsProps.forEach(baseProps::put);
    Map<String, Object> mergedProps = baseProps;
    mergedProps.put(underTest.getJobConfigKey(), config);

    StreamsBuilder streamsBuilder = underTest.buildTopology(
        mergedProps, new StreamsBuilder(), new HashMap<>());
    Properties props = new Properties();
    mergedProps.forEach(props::put);
    mergedProps.put(StreamsConfig.STATE_DIR_CONFIG, file.getAbsolutePath());

    // create topology test driver for ingester
    TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), props);

    Span span = Span.newBuilder().setSpanId(ByteString.copyFrom("1".getBytes()))
        .setTraceId(ByteString.copyFrom("trace-1".getBytes())).build();

    // create input topic for span-normalizer
    Config spanNormalizerConfig = ConfigFactory.parseURL(
        getClass().getClassLoader().getResource("configs/span-normalizer/application.conf"));

    TestInputTopic<byte[], Span> spanNormalizerInputTopic = topologyTestDriver
        .createInputTopic(spanNormalizerConfig.getString(
            org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.INPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().serializer(), new JaegerSpanSerde().serializer());

    spanNormalizerInputTopic.pipeInput(span);

    // create output topic for span-normalizer topology
    TestOutputTopic spanNormalizerOutputTopic = topologyTestDriver
        .createOutputTopic(
            spanNormalizerConfig.getString(StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(),
            new AvroSerde<>().deserializer());
    assertNotNull(spanNormalizerOutputTopic.readKeyValue());

    topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(32));
    Config spanGrouperConfig = ConfigFactory.parseURL(
        getClass().getClassLoader().getResource("configs/raw-spans-grouper/application.conf"));

    // create output topic for span-grouper topology
    TestOutputTopic spanGrouperOutputTopic = topologyTestDriver
        .createOutputTopic(
            spanGrouperConfig.getString(StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(),
            new AvroSerde<>().deserializer());
    assertNotNull(spanGrouperOutputTopic.readKeyValue());

    Config traceEnricherConfig = ConfigFactory.parseURL(
        getClass().getClassLoader().getResource("configs/hypertrace-trace-enricher/application.conf"));

    // create output topic for trace-enricher topology
    TestOutputTopic traceEnricherOutputTopic = topologyTestDriver
        .createOutputTopic(
            traceEnricherConfig.getString(StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(),
            new AvroSerde<>().deserializer());
    assertNotNull(traceEnricherOutputTopic.readKeyValue());

    Config spanEventConfig = ConfigFactory.parseURL(
        getClass().getClassLoader().getResource("configs/view-gen-span-event/application.conf"));

    // create output topic for  topology
    TestOutputTopic spanEventViewOutputTopic = topologyTestDriver
        .createOutputTopic(
            spanEventConfig.getString(StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(),
            new AvroSerde<>().deserializer());
    assertNotNull(spanEventViewOutputTopic.readKeyValue());
  }
}
