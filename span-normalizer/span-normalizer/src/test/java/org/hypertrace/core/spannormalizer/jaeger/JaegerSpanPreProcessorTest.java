package org.hypertrace.core.spannormalizer.jaeger;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Process;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.SpanAttribute;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JaegerSpanPreProcessorTest {

  private final Random random = new Random();

  @Test
  void testPreProcessSpan_missingTenantId() {
    assertThrows(
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

    // provided tenant id in span tags
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

    // provided tenant id in process tags
    process =
        Process.newBuilder()
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .build();

    Span span3 = Span.newBuilder().setProcess(process).build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span3);
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
                List.of("foo:bar,k1:v1", "k2:v2", "http.url:https://foo.bar"))));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().setServiceName("testService").build();
    {
      Span span =
          Span.newBuilder()
              .setProcess(process)
              .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
              .addTags(KeyValue.newBuilder().setKey("foo").setVStr("bar").build())
              .addTags(KeyValue.newBuilder().setKey("k2").setVStr("v2").build())
              .build();
      PreProcessedSpan preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
      // Span dropped due to matching condition: k2:v2
      Assertions.assertNull(preProcessedSpan);
    }

    {
      Span span =
          Span.newBuilder()
              .setProcess(process)
              .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
              .addTags(KeyValue.newBuilder().setKey("foo").setVStr("bar").build())
              .addTags(KeyValue.newBuilder().setKey("http.url").setVStr("https://foo.bar").build())
              .build();
      PreProcessedSpan preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
      // Span dropped due to matching condition: http.url:https://foo.bar
      Assertions.assertNull(preProcessedSpan);
    }

    {
      Span span =
          Span.newBuilder()
              .setProcess(process)
              .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
              .addTags(KeyValue.newBuilder().setKey("foo").setVStr("bar").build())
              .addTags(KeyValue.newBuilder().setKey("http.url").setVStr("https://valid").build())
              .build();
      PreProcessedSpan preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
      // Span not dropped since there is no matching condition
      Assertions.assertNotNull(preProcessedSpan);
    }
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

  @Test
  public void testDropSpan_RootSpan_EmptyExclusionList() {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of(
                "tenantIdTagKey", "tenant-key",
                "rootExitSpanDropCriterion.alwaysDrop", "true")));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().setServiceName("testService").build();

    // root exit span
    Span span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("foo").setVStr("bar").build())
            .addTags(KeyValue.newBuilder().setKey("k2").setVStr("v2").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(SpanAttribute.SPAN_ATTRIBUTE_SPAN_KIND))
                    .setVStr("client")
                    .build())
            .build();
    PreProcessedSpan preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("foo").setVStr("bar").build())
            .addTags(KeyValue.newBuilder().setKey("k2").setVStr("v2").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(SpanAttribute.SPAN_ATTRIBUTE_SPAN_KIND))
                    .setVStr("server")
                    .build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);
  }

  /** Always drop except when there is a match from the exclusion list */
  @Test
  public void testDropSpan_RootSpan_AlwaysDrop_ExclusionList() {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of(
                "tenantIdTagKey", "tenant-key",
                "rootExitSpanDropCriterion.alwaysDrop", "true",
                "rootExitSpanDropCriterion.exclusionsMatchCriterion",
                    List.of("foo:bar,k1:v1", "k2:v2"))));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().setServiceName("testService").build();

    // root exit span
    Span span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("foo").setVStr("bar").build())
            .addTags(KeyValue.newBuilder().setKey("k1").setVStr("v1").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(SpanAttribute.SPAN_ATTRIBUTE_SPAN_KIND))
                    .setVStr("client")
                    .build())
            .build();
    PreProcessedSpan preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("k2").setVStr("v2").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(SpanAttribute.SPAN_ATTRIBUTE_SPAN_KIND))
                    .setVStr("client")
                    .build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("k3").setVStr("v3").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(SpanAttribute.SPAN_ATTRIBUTE_SPAN_KIND))
                    .setVStr("client")
                    .build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);
  }

  /** Always keep except when there is a match from the exclusion list */
  @Test
  public void testDropSpan_RootSpan_NotAlwaysDrop_ExclusionList() {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of(
                "tenantIdTagKey", "tenant-key",
                "rootExitSpanDropCriterion.alwaysDrop", "false",
                "rootExitSpanDropCriterion.exclusionsMatchCriterion",
                    List.of("foo:bar,k1:v1", "k2:v2"))));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().setServiceName("testService").build();

    // root exit span
    Span span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("foo").setVStr("bar").build())
            .addTags(KeyValue.newBuilder().setKey("k1").setVStr("v1").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(SpanAttribute.SPAN_ATTRIBUTE_SPAN_KIND))
                    .setVStr("client")
                    .build())
            .build();
    PreProcessedSpan preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("k2").setVStr("v2").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(SpanAttribute.SPAN_ATTRIBUTE_SPAN_KIND))
                    .setVStr("client")
                    .build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("k3").setVStr("v3").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey(RawSpanConstants.getValue(SpanAttribute.SPAN_ATTRIBUTE_SPAN_KIND))
                    .setVStr("client")
                    .build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);
  }

  @Test
  public void testSpanDropFilters() {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of(
                "tenantIdTagKey",
                "tenant-key",
                "spanDropFilters",
                List.of(
                    List.of(
                        Map.of("tagKey", "http.method", "operator", "EQ", "tagValue", "GET"),
                        Map.of("tagKey", "http.url", "operator", "CONTAINS", "tagValue", "health")),
                    List.of(
                        Map.of(
                            "tagKey", "grpc.url",
                            "operator", "EXISTS",
                            "tagValue", "Sent.TestGrpcService.GetEcho"),
                        Map.of("tagKey", "http.method", "operator", "EQ", "tagValue", "POST")),
                    List.of(
                        Map.of(
                            "tagKey", "http.status_code",
                            "operator", "NEQ",
                            "tagValue", "200"))))));
    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));
    Process process = Process.newBuilder().setServiceName("testService").build();

    // case 1: match first case (http.method & http.url)
    Span span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("http.method").setVStr("GET").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("http.url")
                    .setVStr("http://xyz.com/api/v1/health/check")
                    .build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    PreProcessedSpan preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    // case 2: match second case (grpc.url & http.method)
    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("http.method").setVStr("POST").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("grpc.url")
                    .setVStr("Sent.TestGrpcService.GetEcho.Extra")
                    .build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    // case 3: match third case (http.status_code)
    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("http.status_code").setVStr("500").build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    // case 4 : match no filters but key exists, but value doesn't match
    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("http.method").setVStr("GET").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("http.url")
                    .setVStr("http://xyz.com/api/v1/check")
                    .build())
            .addTags(KeyValue.newBuilder().setKey("http.status_code").setVStr("200").build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);

    // case 4 : match no filters, no key exists
    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);
  }

  @Test
  public void testSpanDropFiltersBadConfig() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Map<String, Object> configs = new HashMap<>(getCommonConfig());
          configs.putAll(
              Map.of(
                  "processor",
                  Map.of(
                      "tenantIdTagKey",
                      "tenant-key",
                      "spanDropFilters",
                      List.of(
                          List.of(
                              Map.of(
                                  "tagKey",
                                  "http.method",
                                  "operator",
                                  "EQUAL",
                                  "tagValue",
                                  "GET"))))));
          JaegerSpanPreProcessor jaegerSpanPreProcessor =
              new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs));
        });
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
