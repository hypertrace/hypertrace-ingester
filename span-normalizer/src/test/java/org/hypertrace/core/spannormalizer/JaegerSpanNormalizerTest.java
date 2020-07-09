package org.hypertrace.core.spannormalizer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Process;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.JaegerAttribute;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanNormalizer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JaegerSpanNormalizerTest {

  @BeforeEach
  public void resetSingleton()
      throws SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException {
    // Since JaegerToRawSpanConverter is a singleton, we need to reset it for unit tests to
    // recreate the instance.
    Field instance = JaegerSpanNormalizer.class.getDeclaredField("INSTANCE");
    instance.setAccessible(true);
    instance.set(null, null);
  }

  @Test
  public void defaultConfigParseTest() {
    try {
      String configPath =
          JaegerSpanNormalizerTest.class
              .getClassLoader()
              .getResource("configs/span-normalizer/test-application.conf")
              .getPath();
      Config jobConfig = ConfigFactory.parseFile(new File(configPath));
      new SpanNormalizerJob(jobConfig);
    } catch (Exception e) {
      // We don't expect any exceptions in parsing the configuration.
      e.printStackTrace();
      Assertions.fail();
    }
  }

  private Map<String, Object> getCommonConfig() {
    return Map.of(
        "span.type",
        "jaeger",
        "flink.job.parallelism",
        4,
        "flink.source",
        Map.of(
            "type", "jaeger",
            "topic", "jaeger-spans",
            "kafka.bootstrap.servers", "localhost:9092"),
        "flink.sink",
        Map.of(
            "type", "kafka",
            "topic", "raw-spans-from-jaeger-spans",
            "kafka.bootstrap.servers", "localhost:9092",
            "schema.registry.schema.registry.url", "http://localhost:8081"),
        "flink.job.metrics.metrics.reporters",
        "prometheus");
  }

  @Test
  public void testTenantIdKeyConfiguration() {
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("tenantIdTagKey", "tenant-id")));
    try {
      new SpanNormalizerJob(ConfigFactory.parseMap(configs));
    } catch (Exception e) {
      // We don't expect any exceptions in parsing the configuration.
      e.printStackTrace();
      Assertions.fail();
    }
  }

  @Test
  public void emptyProcessorConfigShouldFailTheProcessor() {
    Config jobConfig = ConfigFactory.parseMap(getCommonConfig());
    try {
      new SpanNormalizerJob(jobConfig);
      Assertions.fail("config parsing should fail");
    } catch (RuntimeException e) {
      // We expect exception while parsing the config.
      // Trying to be specific with the error message check here so that the test doesn't
      // pass because of some other RuntimeException that happen before we hit the check
      // we want to test.
      Assertions.assertTrue(
          e.getMessage()
              .contains(
                  "Both processor.tenantIdTagKey and processor.defaultTenantId configs can't be null"));
    }
  }

  @Test
  public void invalidConfigCombination1() {
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of(
                "defaultTenantId", "tenant-1",
                "tenantIdTagKey", "tenant-id")));
    try {
      new SpanNormalizerJob(ConfigFactory.parseMap(configs));
      Assertions.fail("config parsing should fail");
    } catch (RuntimeException e) {
      // We expect exception while parsing the config.
      // Trying to be specific with the error message check here so that the test doesn't
      // pass because of some other RuntimeException that happen before we hit the check
      // we want to test.
      Assertions.assertTrue(
          e.getMessage()
              .contains(
                  "Both processor.tenantIdTagKey and processor.defaultTenantId configs shouldn't exist at same time"));
    }
  }

  @Test
  public void testServiceNameAddededToEvent() {
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of(
                "defaultTenantId", "tenant-1")));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().setServiceName("testService").build();
    Span span = Span.newBuilder().setProcess(process).build();
    RawSpan rawSpan = normalizer.convert(span);
    Assertions.assertEquals("testService", rawSpan.getEvent().getServiceName());
    Assertions.assertEquals("testService", rawSpan.getEvent().getAttributes().getAttributeMap().get(
        RawSpanConstants.getValue(JaegerAttribute.JAEGER_ATTRIBUTE_SERVICE_NAME)).getValue());
  }
}
