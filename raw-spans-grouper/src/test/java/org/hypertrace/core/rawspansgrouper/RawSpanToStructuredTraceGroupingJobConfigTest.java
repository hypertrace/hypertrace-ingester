package org.hypertrace.core.rawspansgrouper;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.junit.jupiter.api.Test;

public class RawSpanToStructuredTraceGroupingJobConfigTest {
  @Test
  public void testMinimalCompleteConfig() {
    createRawSpansGrouperJobUsingConfig("minimal-complete-config.conf");
  }

  @Test
  public void testMissingFlinkSourceTopic() {
    assertThrows(ConfigException.Missing.class,
        () -> createRawSpansGrouperJobUsingConfig("missing-flink-source-topic.conf"));
  }

  @Test
  public void testMissingFlinkSinkTopic() {
    assertThrows(ConfigException.Missing.class, () -> createRawSpansGrouperJobUsingConfig("missing-flink-sink-topic.conf"));
  }

  private RawSpanToStructuredTraceGroupingJob createRawSpansGrouperJobUsingConfig(String resourcePath) {
    File configFile = new File(this.getClass().getClassLoader().getResource(resourcePath).getPath());
    Config configs = ConfigFactory.parseFile(configFile);
    return new RawSpanToStructuredTraceGroupingJob(configs);
  }
}
