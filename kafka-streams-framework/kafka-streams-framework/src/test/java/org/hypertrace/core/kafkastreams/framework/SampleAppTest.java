package org.hypertrace.core.kafkastreams.framework;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class SampleAppTest {

  private TopologyTestDriver td;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

  private Topology topology;
  private final Properties config;

  public SampleAppTest() {
    config = new Properties();
    config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, SampleApp.APP_ID);
    config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "foo:1234");
    config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
  }

  @AfterEach
  public void tearDown() {
    td.close();
  }

  @Test
  public void shouldIncludeValueWithLengthGreaterThanFive() {
    topology = SampleApp.retainWordsLongerThan5Letters(new StreamsBuilder()).build();
    td = new TopologyTestDriver(topology, config);

    inputTopic = td.createInputTopic(SampleApp.INPUT_TOPIC, Serdes.String().serializer(),
        Serdes.String().serializer());
    outputTopic = td.createOutputTopic(SampleApp.OUTPUT_TOPIC, Serdes.String().deserializer(),
        Serdes.String().deserializer());

    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("foo", "barrrrr");
    assertThat(outputTopic.readValue(), equalTo("barrrrr"));
    assertThat(outputTopic.isEmpty(), is(true));

    inputTopic.pipeInput("foo", "bar");
    assertThat(outputTopic.isEmpty(), is(true));
  }
}
