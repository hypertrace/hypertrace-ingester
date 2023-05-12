package org.hypertrace.ingester;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.junit.jupiter.api.BeforeEach;

/** Topology Test for {@link HypertraceIngester} */
public class HypertraceIngesterTest {

  private static final String CONFIG_PATH = "configs/%s/application.conf";
  private HypertraceIngester underTest;
  private Config underTestConfig;
  private Config spanNormalizerConfig;
  private Config rawSpansGrouperConfig;
  private Config traceEnricherConfig;
  private Config spanEventViewGeneratorConfig;

  @BeforeEach
  public void setUp() {
    underTest = new HypertraceIngester(ConfigClientFactory.getClient());
    underTestConfig = getConfig("hypertrace-ingester");
    spanNormalizerConfig = getConfig("span-normalizer");
    rawSpansGrouperConfig = getConfig("raw-spans-grouper");
    traceEnricherConfig = getConfig("hypertrace-trace-enricher");
    spanEventViewGeneratorConfig = getConfig("view-gen-span-event");
  }

  //  @Test
  //  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "hypertrace-ingester")
  //  public void testIngestionPacketFlow(@TempDir Path tempDir) {
  //    File file = tempDir.resolve("state").toFile();
  //
  //    Map<String, Object> baseProps = underTest.getBaseStreamsConfig();
  //    Map<String, Object> streamsProps = underTest.getStreamsConfig(underTestConfig);
  //    streamsProps.forEach(baseProps::put);
  //    Map<String, Object> mergedProps = baseProps;
  //    mergedProps.put(underTest.getJobConfigKey(), underTestConfig);
  //
  //    StreamsBuilder streamsBuilder =
  //        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());
  //    Properties props = new Properties();
  //    mergedProps.forEach(props::put);
  //    mergedProps.put(StreamsConfig.STATE_DIR_CONFIG, file.getAbsolutePath());
  //
  //    // create topology test driver for ingester
  //    TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(),
  // props);
  //
  //    Span span =
  //        Span.newBuilder()
  //            .setSpanId(ByteString.copyFrom("1".getBytes()))
  //            .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
  //            .build();
  //
  //    TestInputTopic<byte[], Span> spanNormalizerInputTopic =
  //        topologyTestDriver.createInputTopic(
  //            spanNormalizerConfig.getString(
  //                org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants
  //                    .INPUT_TOPIC_CONFIG_KEY),
  //            Serdes.ByteArray().serializer(),
  //            new JaegerSpanSerde().serializer());
  //
  //    spanNormalizerInputTopic.pipeInput(span);
  //
  //    // create output topic for span-normalizer topology
  //    TestOutputTopic spanNormalizerOutputTopic =
  //        topologyTestDriver.createOutputTopic(
  //            spanNormalizerConfig.getString(
  //                StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY),
  //            Serdes.String().deserializer(),
  //            new AvroSerde<>().deserializer());
  //    assertNotNull(spanNormalizerOutputTopic.readKeyValue());
  //
  //    topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(32));
  //
  //    // create output topic for span-grouper topology
  //    TestOutputTopic spanGrouperOutputTopic =
  //        topologyTestDriver.createOutputTopic(
  //            rawSpansGrouperConfig.getString(
  //                StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY),
  //            Serdes.String().deserializer(),
  //            new AvroSerde<>().deserializer());
  //    assertNotNull(spanGrouperOutputTopic.readKeyValue());
  //
  //    // create output topic for trace-enricher topology
  //    TestOutputTopic traceEnricherOutputTopic =
  //        topologyTestDriver.createOutputTopic(
  //
  // traceEnricherConfig.getString(StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY),
  //            Serdes.String().deserializer(),
  //            new AvroSerde<>().deserializer());
  //    assertNotNull(traceEnricherOutputTopic.readKeyValue());
  //
  //    // create output topic for  topology
  //    TestOutputTopic spanEventViewOutputTopic =
  //        topologyTestDriver.createOutputTopic(
  //            spanEventViewGeneratorConfig.getString(
  //                StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY),
  //            Serdes.String().deserializer(),
  //            new AvroSerde<>().deserializer());
  //    SpanEventView spanEventView = (SpanEventView) spanEventViewOutputTopic.readValue();
  //    assertEquals(
  //        HexUtils.getHex(spanEventView.getSpanId()),
  //        HexUtils.getHex(span.getSpanId().toByteArray()));
  //    assertEquals(
  //        HexUtils.getHex(spanEventView.getTraceId()),
  //        HexUtils.getHex(span.getTraceId().toByteArray()));
  //  }

  private Config getConfig(String serviceName) {
    return ConfigFactory.parseURL(
        getClass().getClassLoader().getResource(String.format(CONFIG_PATH, serviceName)));
  }
}
