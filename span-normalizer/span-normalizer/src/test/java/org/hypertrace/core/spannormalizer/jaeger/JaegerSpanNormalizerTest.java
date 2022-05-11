package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_METHOD;
import static org.hypertrace.core.spannormalizer.util.EventBuilder.buildEvent;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Process;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.JaegerAttribute;
import org.hypertrace.core.spannormalizer.SpanNormalizer;
import org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants;
import org.hypertrace.core.spannormalizer.utils.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class JaegerSpanNormalizerTest {

  private final Random random = new Random();

  @BeforeAll
  static void initializeMetricRegistry() {
    // Initialize the metric registry.
    PlatformMetricsRegistry.initMetricsRegistry(
        "JaegerSpanNormalizerTest",
        ConfigFactory.parseMap(Map.of("reporter.names", List.of("testing"))));
  }

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
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "span-normalizer")
  public void defaultConfigParseTest() {
    try {
      new SpanNormalizer(ConfigClientFactory.getClient());
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
        "input.topic",
        "jaeger-spans",
        "output.topic",
        "raw-spans-from-jaeger-spans",
        "kafka.streams.config",
        Map.of(
            "application.id",
            "jaeger-spans-to-raw-spans-job",
            "bootstrap.servers",
            "localhost:9092"),
        "schema.registry.config",
        Map.of("schema.registry.url", "http://localhost:8081"),
        "piiFields",
        List.of("http.method", "http.url", "amount", "Authorization"));
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "span-normalizer")
  public void testTenantIdKeyConfiguration() {
    try {
      new SpanNormalizer(ConfigClientFactory.getClient());
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
      JaegerSpanNormalizer.get(jobConfig);
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
      JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));
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
  public void testServiceNameAddedToEvent() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().setServiceName("testService").build();
    Span span = Span.newBuilder().setProcess(process).build();
    RawSpan rawSpan =
        normalizer.convert(tenantId, span, buildEvent(tenantId, span, Optional.empty()));
    Assertions.assertEquals("testService", rawSpan.getEvent().getServiceName());
    Assertions.assertEquals(
        "testService",
        rawSpan
            .getEvent()
            .getAttributes()
            .getAttributeMap()
            .get(RawSpanConstants.getValue(JaegerAttribute.JAEGER_ATTRIBUTE_SERVICE_NAME))
            .getValue());

    /**
     * the case when `jaegerSpan.getProcess().getServiceName()` is not populated but serviceName is
     * sent for key `jaeger.serviceName`
     */
    span =
        Span.newBuilder()
            .addTags(
                KeyValue.newBuilder()
                    .setKey(JaegerSpanNormalizer.OLD_JAEGER_SERVICENAME_KEY)
                    .setVStr("testService"))
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(HTTP_REQUEST_METHOD))
                    .setVStr("GET"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span, buildEvent(tenantId, span, Optional.empty()));
    Assertions.assertEquals("testService", rawSpan.getEvent().getServiceName());
    Assertions.assertEquals(
        "testService",
        rawSpan
            .getEvent()
            .getAttributes()
            .getAttributeMap()
            .get(RawSpanConstants.getValue(JaegerAttribute.JAEGER_ATTRIBUTE_SERVICE_NAME))
            .getValue());

    /**
     * the case when neither `jaegerSpan.getProcess().getServiceName()` nor `jaeger.serviceName` is
     * present in tag map
     */
    span =
        Span.newBuilder()
            .addTags(KeyValue.newBuilder().setKey("someKey").setVStr("someValue"))
            .build();
    rawSpan = normalizer.convert("tenant-key", span, buildEvent(tenantId, span, Optional.empty()));
    Assertions.assertNull(rawSpan.getEvent().getServiceName());
    Assertions.assertNotNull(rawSpan.getEvent().getAttributes().getAttributeMap());
    Assertions.assertNull(
        rawSpan
            .getEvent()
            .getAttributes()
            .getAttributeMap()
            .get(RawSpanConstants.getValue(JaegerAttribute.JAEGER_ATTRIBUTE_SERVICE_NAME)));
  }

  @Test
  public void testServiceNameNotAddedToEvent() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().build();
    Span span = Span.newBuilder().setProcess(process).build();

    RawSpan rawSpan =
        normalizer.convert(tenantId, span, buildEvent(tenantId, span, Optional.empty()));
    Assertions.assertNull(rawSpan.getEvent().getServiceName());
    Assertions.assertNull(
        rawSpan
            .getEvent()
            .getAttributes()
            .getAttributeMap()
            .get(RawSpanConstants.getValue(JaegerAttribute.JAEGER_ATTRIBUTE_SERVICE_NAME)));
    Timer timer = normalizer.getSpanNormalizationTimer(tenantId);
    Assertions.assertNotNull(timer);

    // Assert that metrics are collected.
    Assertions.assertEquals(1, timer.count());
  }

  @Test
  public void testConvertToJsonString() throws IOException {
    AttributeValue attributeValue = AttributeValue.newBuilder().setValue("test-val").build();
    Assertions.assertEquals(
        "{\"value\":{\"string\":\"test-val\"},\"binary_value\":null,\"value_list\":null,\"value_map\":null}",
        JaegerSpanNormalizer.convertToJsonString(attributeValue, AttributeValue.getClassSchema()));
  }

  @Test
  public void testPiiFieldRedaction() throws Exception {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().build();
    Span span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(0, KeyValue.newBuilder().setKey("http.method").setVStr("GET").build())
            .addTags(1, KeyValue.newBuilder().setKey("http.url").setVStr("hypertrace.org"))
            .addTags(2, KeyValue.newBuilder().setKey("kind").setVStr("client"))
            .addTags(3, KeyValue.newBuilder().setKey("authorization").setVStr("authToken").build())
            .addTags(4, KeyValue.newBuilder().setKey("amount").setVInt64(2300).build())
            .build();

    RawSpan rawSpan =
        normalizer.convert(tenantId, span, buildEvent(tenantId, span, Optional.empty()));

    var attributes = rawSpan.getEvent().getAttributes().getAttributeMap();

    Assertions.assertEquals("client", attributes.get("kind").getValue());
    Assertions.assertEquals(
        SpanNormalizerConstants.PII_FIELD_REDACTED_VAL, attributes.get("http.url").getValue());
    Assertions.assertEquals(
        SpanNormalizerConstants.PII_FIELD_REDACTED_VAL, attributes.get("http.method").getValue());
    Assertions.assertEquals(
        SpanNormalizerConstants.PII_FIELD_REDACTED_VAL, attributes.get("amount").getValue());
    Assertions.assertEquals(
        SpanNormalizerConstants.PII_FIELD_REDACTED_VAL, attributes.get("authorization").getValue());
  }

  @Test
  public void testPiiFieldRedactionWithNoConfig() throws Exception {
    String tenantId = "tenant-" + random.nextLong();

    Map<String, Object> config =
        Map.of(
            "span.type",
            "jaeger",
            "input.topic",
            "jaeger-spans",
            "output.topic",
            "raw-spans-from-jaeger-spans",
            "kafka.streams.config",
            Map.of(
                "application.id",
                "jaeger-spans-to-raw-spans-job",
                "bootstrap.servers",
                "localhost:9092"),
            "schema.registry.config",
            Map.of("schema.registry.url", "http://localhost:8081"));

    Map<String, Object> configs = new HashMap<>(config);
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", tenantId)));
    JaegerSpanNormalizer normalizer = JaegerSpanNormalizer.get(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().build();
    Span span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(0, TestUtils.createKeyValue("http.method", "GET"))
            .addTags(1, TestUtils.createKeyValue("http.url", "hypertrace.org"))
            .addTags(2, TestUtils.createKeyValue("kind", "client"))
            .addTags(3, TestUtils.createKeyValue("authorization", "authToken"))
            .addTags(4, TestUtils.createKeyValue("amount", 2300))
            .build();

    RawSpan rawSpan =
        normalizer.convert(tenantId, span, buildEvent(tenantId, span, Optional.empty()));

    var attributes = rawSpan.getEvent().getAttributes().getAttributeMap();

    Assertions.assertEquals("client", attributes.get("kind").getValue());
    Assertions.assertEquals("hypertrace.org", attributes.get("http.url").getValue());
    Assertions.assertEquals("GET", attributes.get("http.method").getValue());
    Assertions.assertEquals(2300, Integer.valueOf(attributes.get("amount").getValue()));
    Assertions.assertEquals("authToken", attributes.get("authorization").getValue());
  }
}
