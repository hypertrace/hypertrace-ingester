import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants;
import org.hypertrace.traceenricher.trace.enricher.TraceEnricher;
import org.junit.jupiter.api.Assertions;
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
    underTestConfig = ConfigFactory.parseURL(
        getClass().getClassLoader()
            .getResource("configs/hypertrace-trace-enricher/application.conf"))
        .resolve();
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "hypertrace-trace-enricher")
  public void testTraceEnricherTopology() {
    Config config = ConfigFactory.parseURL(
        getClass().getClassLoader()
            .getResource("configs/hypertrace-trace-enricher/application.conf")).resolve();

    Map<String, Object> baseProps = underTest.getBaseStreamsConfig();
    Map<String, Object> streamsProps = underTest.getStreamsConfig(config);
    streamsProps.forEach(baseProps::put);
    Map<String, Object> mergedProps = baseProps;
    mergedProps.put(underTest.getJobConfigKey(), config);

    StreamsBuilder streamsBuilder = underTest.buildTopology(
        mergedProps, new StreamsBuilder(), new HashMap<>());
    Properties props = new Properties();
    mergedProps.forEach(props::put);

    // create topology test driver for trace-enricher
    TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), props);

    Serde<StructuredTrace> htStructuredTraceSerde = new AvroSerde<>();

    // create input topic for HT-model StructuredTrace
    TestInputTopic<String, StructuredTrace> inputTopic =
        topologyTestDriver
            .createInputTopic(
                config.getString(StructuredTraceEnricherConstants.INPUT_TOPIC_CONFIG_KEY),
                Serdes.String().serializer(),
                htStructuredTraceSerde.serializer());

    // create output topic for closed-model StructuredTrace
    TestOutputTopic outputTopic = topologyTestDriver
        .createOutputTopic(
            config.getString(StructuredTraceEnricherConstants.OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.String().deserializer(),
            htStructuredTraceSerde.deserializer());

    // create instance of HT-model StructuredTrace
    StructuredTrace htStructuredTrace =
        createHTStructuredTrace("customer1", "1234");

    // Write an input record into input topic
    inputTopic.pipeInput(htStructuredTrace);

    // Read the output record from output topic
    StructuredTrace structuredTrace = (StructuredTrace) outputTopic.readValue();

    Assertions.assertEquals(HexUtils.getHex("1234".getBytes()),
        HexUtils.getHex(structuredTrace.getTraceId()));
  }

  private org.hypertrace.core.datamodel.StructuredTrace createHTStructuredTrace(
      String customerId, String traceId) {
    return StructuredTrace.newBuilder()
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
  }
}
