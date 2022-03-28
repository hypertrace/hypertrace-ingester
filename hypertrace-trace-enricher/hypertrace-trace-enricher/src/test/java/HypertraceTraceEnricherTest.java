import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants;
import org.hypertrace.traceenricher.trace.enricher.TraceEnricher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class HypertraceTraceEnricherTest {

  private TraceEnricher underTest;
  private Config underTestConfig;

  @BeforeEach
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "hypertrace-trace-enricher")
  public void setUp() {
    underTest = new TraceEnricher(ConfigClientFactory.getClient());
    underTestConfig =
        ConfigFactory.parseURL(
                getClass()
                    .getClassLoader()
                    .getResource("configs/hypertrace-trace-enricher/application.conf"))
            .resolve();
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "hypertrace-trace-enricher")
  public void testTraceEnricherTopology() {
    Config config =
        ConfigFactory.parseURL(
                getClass()
                    .getClassLoader()
                    .getResource("configs/hypertrace-trace-enricher/application.conf"))
            .resolve();

    Map<String, Object> baseProps = underTest.getBaseStreamsConfig();
    Map<String, Object> streamsProps = underTest.getStreamsConfig(config);
    streamsProps.forEach(baseProps::put);
    Map<String, Object> mergedProps = baseProps;
    mergedProps.put(underTest.getJobConfigKey(), config);

    StreamsBuilder streamsBuilder =
        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());
    Properties props = new Properties();
    mergedProps.forEach(props::put);

    // create topology test driver for trace-enricher
    TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), props);

    Serde<TraceIdentity> traceKeySerde = new AvroSerde<>();
    Serde<StructuredTrace> htStructuredTraceSerde = new AvroSerde<>();

    // create input topic for HT-model StructuredTrace
    TestInputTopic<TraceIdentity, StructuredTrace> inputTopic =
        topologyTestDriver.createInputTopic(
            config.getString(StructuredTraceEnricherConstants.INPUT_TOPIC_CONFIG_KEY),
            traceKeySerde.serializer(),
            htStructuredTraceSerde.serializer());

    // create output topic for closed-model StructuredTrace
    TestOutputTopic<String, StructuredTrace> outputTopic =
        topologyTestDriver.createOutputTopic(
            config.getString(StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(),
            htStructuredTraceSerde.deserializer());

    // create instance of HT-model StructuredTrace
    KeyValue<TraceIdentity, StructuredTrace> traceKVPair1 =
        createHTStructuredTrace("customer1", "1234");
    KeyValue<TraceIdentity, StructuredTrace> traceKVPair2 =
        createHTStructuredTrace("customer2", "5678");

    // Write an input record into input topic
    inputTopic.pipeInput(traceKVPair1.key, traceKVPair1.value);
    // enricher should be able to handle null keys as well for backward compatibility
    inputTopic.pipeInput(null, traceKVPair2.value);

    // Read the output record from output topic
    StructuredTrace structuredTrace1 = outputTopic.readValue();
    StructuredTrace structuredTrace2 = outputTopic.readValue();

    assertEquals(
        HexUtils.getHex("1234".getBytes()), HexUtils.getHex(structuredTrace1.getTraceId()));
    assertEquals(
        HexUtils.getHex("5678".getBytes()), HexUtils.getHex(structuredTrace2.getTraceId()));
  }

  private KeyValue<TraceIdentity, StructuredTrace> createHTStructuredTrace(
      String customerId, String traceId) {
    TraceIdentity traceKey =
        TraceIdentity.newBuilder()
            .setTenantId(customerId)
            .setTraceId(ByteBuffer.wrap(traceId.getBytes()))
            .build();
    StructuredTrace trace =
        StructuredTrace.newBuilder()
            .setCustomerId(customerId)
            .setTraceId(ByteBuffer.wrap(traceId.getBytes()))
            .setStartTimeMillis(System.currentTimeMillis() - 10000)
            .setEndTimeMillis(System.currentTimeMillis())
            .setAttributes(Attributes.newBuilder().build())
            .setEntityList(new ArrayList<>())
            .setEntityEdgeList(new ArrayList<>())
            .setEventEdgeList(new ArrayList<>())
            .setEntityEventEdgeList(new ArrayList<>())
            .setEventList(new ArrayList<>())
            .build();
    return KeyValue.pair(traceKey, trace);
  }
}
