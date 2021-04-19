package org.hypertrace.core.spannormalizer.jaeger;

import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Process;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JaegerSpanPreProcessorTest {

  private final Random random = new Random();

  @Test
  void testPreProcessSpan_missingTenantId() {
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          // span dropped since tenant detail not present
          String tenantId = "tenant-" + random.nextLong();
          Map<String, Object> configs = new HashMap<>(getCommonConfig());
          configs.putAll(Map.of("processor", Map.of()));
          JaegerSpanPreProcessor jaegerSpanPreProcessor =
              new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));

          Process process = Process.newBuilder().setServiceName("testService").build();
          Span span1 =
              Span.newBuilder()
                  .setProcess(process)
                  .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
                  .build();
          jaegerSpanPreProcessor.preProcessSpan(span1);
        });
  }

  @Test
  void testPreProcessSpan_validTenantId() {
    // default tenant id
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("defaultTenantId", "default-tenant")));
    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));

    Process process = Process.newBuilder().setServiceName("testService").build();
    Span span1 =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("key").setVStr("Val").build())
            .build();
    PreProcessedSpan preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span1);
    Assertions.assertEquals("default-tenant", preProcessedSpan.getTenantId());

    // provided tenant id
    configs = new HashMap<>(getCommonConfig());
    configs.putAll(Map.of("processor", Map.of("tenantIdTagKey", "tenant-key")));
    jaegerSpanPreProcessor = new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));

    Span span2 =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span2);
    Assertions.assertEquals(tenantId, preProcessedSpan.getTenantId());
  }

  @Test
  void testPreProcessSpan_excludeTenantId() {
    String tenantId = "tenant-" + random.nextLong();
    String anotherTenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of("tenantIdTagKey", "tenant-key", "excludeTenantIds", List.of(tenantId))));
    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));

    Process process = Process.newBuilder().setServiceName("testService").build();
    Span span1 =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .build();
    PreProcessedSpan preProcessedSpan1 = jaegerSpanPreProcessor.preProcessSpan(span1);
    Assertions.assertNull(preProcessedSpan1);

    Span span2 =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(anotherTenantId).build())
            .build();
    PreProcessedSpan preProcessedSpan2 = jaegerSpanPreProcessor.preProcessSpan(span2);
    Assertions.assertNotNull(preProcessedSpan2);
  }

  @Test
  public void testSpanDropCriterion() {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of("tenantIdTagKey", "tenant-key", "spanDropCriterion", List.of("foo:bar,k1:v1"))));
    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().setServiceName("testService").build();
    Span span1 =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("foo").setVStr("bar").build())
            .build();
    PreProcessedSpan preProcessedSpan1 = jaegerSpanPreProcessor.preProcessSpan(span1);
    Assertions.assertNotNull(preProcessedSpan1);

    Span span2 =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("foo").setVStr("bar").build())
            .addTags(KeyValue.newBuilder().setKey("k1").setVStr("v1").build())
            .build();
    PreProcessedSpan preProcessedSpan2 = jaegerSpanPreProcessor.preProcessSpan(span2);
    Assertions.assertNull(preProcessedSpan2);
  }

  @Test
  public void testDropSpanWithMultipleCriterion() {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of(
                "tenantIdTagKey",
                "tenant-key",
                "spanDropCriterion",
                List.of("foo:bar,k1:v1", "k2:v2"))));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().setServiceName("testService").build();
    Span span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("foo").setVStr("bar").build())
            .addTags(KeyValue.newBuilder().setKey("k2").setVStr("v2").build())
            .build();
    PreProcessedSpan preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);
  }

  @Test
  public void testDropSpanWithEmptyCriterion() {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor", Map.of("tenantIdTagKey", "tenant-key", "spanDropCriterion", List.of())));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().setServiceName("testService").build();
    Span span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("foo").setVStr("bar").build())
            .addTags(KeyValue.newBuilder().setKey("k2").setVStr("v2").build())
            .build();
    PreProcessedSpan preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);
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
        Map.of("schema.registry.url", "http://localhost:8081"));
  }
}
