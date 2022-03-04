package org.hypertrace.core.spannormalizer.jaeger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Timestamp;
import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Process;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.SpanAttribute;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRuleInfo;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.LogicalOperator;
import org.hypertrace.span.processing.config.service.v1.LogicalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JaegerSpanPreProcessorTest {

  private final Random random = new Random();
  private ExcludeSpanRulesCache excludeSpanRulesCache;

  @BeforeEach
  void init() throws ExecutionException {
    excludeSpanRulesCache = mock(ExcludeSpanRulesCache.class);
    when(excludeSpanRulesCache.get(any())).thenReturn(Collections.emptyList());
  }

  @Test
  void testPreProcessSpan_missingTenantId() {
    assertThrows(
        RuntimeException.class,
        () -> {
          // span dropped since tenant detail not present
          String tenantId = "tenant-" + random.nextLong();
          Map<String, Object> configs = new HashMap<>(getCommonConfig());
          configs.putAll(Map.of("processor", Map.of("late.arrival.threshold.duration", "1d")));
          JaegerSpanPreProcessor jaegerSpanPreProcessor =
              new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);

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
    configs.putAll(
        Map.of(
            "processor",
            Map.of("defaultTenantId", "default-tenant", "late.arrival.threshold.duration", "1d")));
    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);

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
    configs.putAll(
        Map.of(
            "processor",
            Map.of("tenantIdTagKey", "tenant-key", "late.arrival.threshold.duration", "1d")));
    jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);

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
            Map.of(
                "tenantIdTagKey",
                "tenant-key",
                "excludeTenantIds",
                List.of(tenantId),
                "late.arrival.threshold.duration",
                "1d")));
    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);

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
            Map.of(
                "tenantIdTagKey",
                "tenant-key",
                "spanDropCriterion",
                List.of("foo:bar,k1:v1"),
                "late.arrival.threshold.duration",
                "1d")));
    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);
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
                List.of("foo:bar,k1:v1", "k2:v2", "http.url:https://foo.bar"),
                "late.arrival.threshold.duration",
                "1d")));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);
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
            "processor",
            Map.of(
                "tenantIdTagKey",
                "tenant-key",
                "spanDropCriterion",
                List.of(),
                "late.arrival.threshold.duration",
                "1d")));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);
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
                "rootExitSpanDropCriterion.alwaysDrop", "true",
                "late.arrival.threshold.duration", "1d")));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);
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
                    List.of("foo:bar,k1:v1", "k2:v2"),
                "late.arrival.threshold.duration", "1d")));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);
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
                    List.of("foo:bar,k1:v1", "k2:v2"),
                "late.arrival.threshold.duration", "1d")));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);
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
                "late.arrival.threshold.duration",
                "1d",
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
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);
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
  public void testSpanDropFiltersWithCombinationOfProcessAndSpanTags() {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of(
                "tenantIdTagKey",
                "tenant-key",
                "late.arrival.threshold.duration",
                "1d",
                "spanDropFilters",
                List.of(
                    List.of(
                        Map.of("tagKey", "tenant-key", "operator", "EQ", "tagValue", tenantId),
                        Map.of("tagKey", "http.method", "operator", "EQ", "tagValue", "GET"),
                        Map.of("tagKey", "http.url", "operator", "CONTAINS", "tagValue", "health")),
                    List.of(
                        Map.of(
                            "tagKey",
                            "service_name",
                            "operator",
                            "CONTAINS",
                            "tagValue",
                            "drop-service"),
                        Map.of(
                            "tagKey", "grpc.url", "operator", "EXISTS", "tagValue", "health"))))));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);

    // case 1: {spanTags: [http.method & http.url],  processTags:tenant_id } matches -> drop span

    Process process =
        Process.newBuilder()
            .setServiceName("testService")
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .build();

    Span span =
        Span.newBuilder()
            .setProcess(process)
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

    // case 2: {spanTags: [http.method & http.url],  processTags:tenant_id } not matches tenantId
    process =
        Process.newBuilder()
            .setServiceName("testService")
            .addTags(
                KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId + "not-match").build())
            .build();

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("http.method").setVStr("GET").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("http.url")
                    .setVStr("http://xyz.com/api/v1/health/check")
                    .build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);

    // case 3: {spanTags: [http.method & http.url & tenant_id],  processTags:tenant_id }
    // match with spanTag's tenantId -> Drop span
    process =
        Process.newBuilder()
            .setServiceName("testService")
            .addTags(
                KeyValue.newBuilder()
                    .setKey("tenant-key")
                    .setVStr(tenantId + "not-match-process")
                    .build())
            .build();

    span =
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
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    // case 4: {spanTags: [http.method & http.url & tenant_id],  processTags:tenant_id }
    // not match with spanTag or processTag tenantId
    process =
        Process.newBuilder()
            .setServiceName("testService")
            .addTags(
                KeyValue.newBuilder()
                    .setKey("tenant-key")
                    .setVStr(tenantId + "not-match-process")
                    .build())
            .build();

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(
                KeyValue.newBuilder()
                    .setKey("tenant-key")
                    .setVStr(tenantId + "not-match-span")
                    .build())
            .addTags(KeyValue.newBuilder().setKey("http.method").setVStr("GET").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("http.url")
                    .setVStr("http://xyz.com/api/v1/health/check")
                    .build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);

    // case 5: {spanTags: [!grpc.url],  processTags:service_name }
    // contains test for processTags -> matches
    // grpc.url exists in spanTags -> matches
    process =
        Process.newBuilder()
            .setServiceName("testService")
            .addTags(
                KeyValue.newBuilder()
                    .setKey("service_name")
                    .setVStr("drop-service-payment")
                    .build())
            .build();

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(
                KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId + "not-match-span"))
            .addTags(KeyValue.newBuilder().setKey("http.method").setVStr("GET").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("grpc.url")
                    .setVStr("http://xyz.com/api/v1/health/check")
                    .build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    // case 6: {spanTags: [!grpc.url],  processTags:service_name }
    // contains test for processTags -> doesn't matches
    // grpc.url exists in spanTags -> match
    process =
        Process.newBuilder()
            .setServiceName("testService")
            .addTags(KeyValue.newBuilder().setKey("service_name").setVStr("payment").build())
            .build();

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(
                KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId + "not-match-span"))
            .addTags(KeyValue.newBuilder().setKey("http.url").setVStr("GET").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("grpc.url")
                    .setVStr("http://xyz.com/api/v1/health/check")
                    .build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);

    // case 6: {spanTags: [!grpc.url],  processTags:service_name }
    // contains test for processTags -> doesn't matches
    // grpc.url doesn't exists in spanTags
    process =
        Process.newBuilder()
            .setServiceName("testService")
            .addTags(KeyValue.newBuilder().setKey("service_name").setVStr("payment").build())
            .build();

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(
                KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId + "not-match-span"))
            .addTags(KeyValue.newBuilder().setKey("http.url").setVStr("GET").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("http.url")
                    .setVStr("http://xyz.com/api/v1/health/check")
                    .build())
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
                      "late.arrival.threshold.duration",
                      "1d",
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
              new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);
        });
  }

  @Test
  public void testExcludeSpanRules() throws ExecutionException {
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of(
                "tenantIdTagKey",
                "tenant-key",
                "spanDropFilters",
                Collections.emptyList(),
                "late.arrival.threshold.duration",
                "1d")));

    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);

    // case 1: {spanTags: [http.method & http.url],  processTags:tenant_id, rule: url contains
    // health } matches -> drop span
    when(excludeSpanRulesCache.get(any()))
        .thenReturn(
            List.of(
                ExcludeSpanRule.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setFilter(
                                buildRelationalFilter(
                                    Field.FIELD_URL,
                                    null,
                                    RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                                    "health"))
                            .build())
                    .build()));

    Process process =
        Process.newBuilder()
            .setServiceName("testService")
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .build();

    Span span =
        Span.newBuilder()
            .setProcess(process)
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

    // case 2: {spanTags: [http.url],  processTags:tenant_id, url contains health }
    // should not drop span
    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(
                KeyValue.newBuilder()
                    .setKey("http.url")
                    .setVStr("http://xyz.com/api/v1/healt/check")
                    .build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);

    // case 3: {spanTags: [http.url & tenant_id],  processTags:tenant_id, rule - service name is
    // testService } drop span
    when(excludeSpanRulesCache.get(any()))
        .thenReturn(
            List.of(
                ExcludeSpanRule.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setFilter(
                                buildRelationalFilter(
                                    Field.FIELD_SERVICE_NAME,
                                    null,
                                    RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                                    "testService"))
                            .build())
                    .build()));
    process =
        Process.newBuilder()
            .setServiceName("testService")
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .build();

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("http.url")
                    .setVStr("http://xyz.com/api/v1/health/check")
                    .build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    // case 4: {spanTags: [http.method & http.request.url],  processTags:tenant_id, url:
    //   service name is testService and url contains health } drop span
    when(excludeSpanRulesCache.get(any()))
        .thenReturn(
            List.of(
                ExcludeSpanRule.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setFilter(
                                buildLogicalFilterSpanProcessing(
                                    LogicalOperator.LOGICAL_OPERATOR_AND,
                                    List.of(
                                        buildRelationalFilter(
                                            Field.FIELD_SERVICE_NAME,
                                            null,
                                            RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                                            "testService"),
                                        buildRelationalFilter(
                                            Field.FIELD_URL,
                                            null,
                                            RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                                            "health"))))
                            .build())
                    .build()));
    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("http.method").setVStr("GET").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("http.request.url")
                    .setVStr("http://xyz.com/api/v1/health/check")
                    .build())
            .addTags(KeyValue.newBuilder().setKey("extra.tag").setVStr("extra-test-value").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    // same as above but filter fails - should not drop span
    when(excludeSpanRulesCache.get(any()))
        .thenReturn(
            List.of(
                ExcludeSpanRule.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setFilter(
                                buildLogicalFilterSpanProcessing(
                                    LogicalOperator.LOGICAL_OPERATOR_AND,
                                    List.of(
                                        buildRelationalFilter(
                                            Field.FIELD_SERVICE_NAME,
                                            null,
                                            RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                                            "testServiceAttribute"),
                                        buildRelationalFilter(
                                            Field.FIELD_URL,
                                            null,
                                            RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                                            "health"))))
                            .build())
                    .build()));

    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);

    // case5 : {[http.url, jaeger.servicename], rule -> same as above} drop span
    process =
        Process.newBuilder()
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .build();

    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId))
            .addTags(
                KeyValue.newBuilder()
                    .setKey("http.url")
                    .setVStr("http://xyz.com/api/v1/health/check")
                    .build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr("testServiceAttribute")
                    .build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    // case6 : {[http.url, deployment.environment], rule -> env equals env1 and url contains health}
    // drop span
    when(excludeSpanRulesCache.get(any()))
        .thenReturn(
            List.of(
                ExcludeSpanRule.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setFilter(
                                buildLogicalFilterSpanProcessing(
                                    LogicalOperator.LOGICAL_OPERATOR_AND,
                                    List.of(
                                        buildRelationalFilter(
                                            Field.FIELD_ENVIRONMENT_NAME,
                                            null,
                                            RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                                            "env1"),
                                        buildRelationalFilter(
                                            Field.FIELD_URL,
                                            null,
                                            RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                                            "health"))))
                            .build())
                    .build()));
    span =
        Span.newBuilder()
            .setProcess(process)
            .addTags(KeyValue.newBuilder().setKey("tenant-key").setVStr(tenantId).build())
            .addTags(KeyValue.newBuilder().setKey("deployment.environment").setVStr("env1").build())
            .addTags(
                KeyValue.newBuilder()
                    .setKey("http.request.url")
                    .setVStr("http://xyz.com/api/v1/health/check")
                    .build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);

    // case7 : {[http.url, deployment.environment], rule -> env equals env1 and url contains health}
    // drop span
    when(excludeSpanRulesCache.get(any()))
        .thenReturn(
            List.of(
                ExcludeSpanRule.newBuilder()
                    .setRuleInfo(
                        ExcludeSpanRuleInfo.newBuilder()
                            .setFilter(
                                buildLogicalFilterSpanProcessing(
                                    LogicalOperator.LOGICAL_OPERATOR_AND,
                                    List.of(
                                        buildRelationalFilter(
                                            null,
                                            "deployment.environment",
                                            RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                                            "env1"),
                                        buildRelationalFilter(
                                            Field.FIELD_URL,
                                            null,
                                            RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                                            "health"))))
                            .build())
                    .build()));
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);
  }

  @Test
  public void testLateArrivalSpanWithConfiguredConfig() {
    // case 1: 24 hrs config, span within range, should not drop
    String tenantId = "tenant-" + random.nextLong();
    Map<String, Object> configs = new HashMap<>(getCommonConfig());
    configs.putAll(
        Map.of(
            "processor",
            Map.of("defaultTenantId", tenantId, "late.arrival.threshold.duration", "24h")));
    JaegerSpanPreProcessor jaegerSpanPreProcessor =
        new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);

    Instant instant = Instant.now();
    Process process = Process.newBuilder().setServiceName("testService").build();
    Span span =
        Span.newBuilder()
            .setProcess(process)
            .setStartTime(Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).build())
            .addTags(KeyValue.newBuilder().setKey("key").setVStr("Val").build())
            .build();
    PreProcessedSpan preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNotNull(preProcessedSpan);
    Assertions.assertEquals(tenantId, preProcessedSpan.getTenantId());

    // case 2: 24 hrs config, span too old, more than 25hrs, should drop
    instant = Instant.now().minus(25, ChronoUnit.HOURS);
    process = Process.newBuilder().setServiceName("testService").build();
    span =
        Span.newBuilder()
            .setProcess(process)
            .setStartTime(Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).build())
            .addTags(KeyValue.newBuilder().setKey("key").setVStr("Val").build())
            .build();
    preProcessedSpan = jaegerSpanPreProcessor.preProcessSpan(span);
    Assertions.assertNull(preProcessedSpan);
  }

  @Test
  public void testBadLateArrivalSpanConfig() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Map<String, Object> configs = new HashMap<>(getCommonConfig());
          configs.putAll(
              Map.of(
                  "processor",
                  Map.of(
                      "tenantIdTagKey", "tenant-key", "late.arrival.threshold.duration", "20s")));
          JaegerSpanPreProcessor jaegerSpanPreProcessor =
              new JaegerSpanPreProcessor(ConfigFactory.parseMap(configs), excludeSpanRulesCache);
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
        Map.of("schema.registry.url", "http://localhost:8081"),
        "clients",
        Map.of("config.service.config", Map.of("host", "localhost", "port", 50101)),
        "span.rules.exclude.cache",
        Map.of("refreshAfterWriteDuration", "3m", "expireAfterWriteDuration", "5m"));
  }

  private static SpanFilter buildRelationalFilter(
      Field field, String spanAttributeKey, RelationalOperator operator, String rhs) {
    RelationalSpanFilterExpression.Builder relationalSpanFilterExpressionBuilder =
        RelationalSpanFilterExpression.newBuilder();
    if (spanAttributeKey == null) {
      relationalSpanFilterExpressionBuilder.setField(field);
    } else {
      relationalSpanFilterExpressionBuilder.setSpanAttributeKey(spanAttributeKey);
    }
    return SpanFilter.newBuilder()
        .setRelationalSpanFilter(
            relationalSpanFilterExpressionBuilder
                .setOperator(operator)
                .setRightOperand(SpanFilterValue.newBuilder().setStringValue(rhs).build())
                .build())
        .build();
  }

  private static SpanFilter buildLogicalFilterSpanProcessing(
      org.hypertrace.span.processing.config.service.v1.LogicalOperator operator,
      List<org.hypertrace.span.processing.config.service.v1.SpanFilter> filters) {
    return SpanFilter.newBuilder()
        .setLogicalSpanFilter(
            LogicalSpanFilterExpression.newBuilder()
                .setOperator(operator)
                .addAllOperands(filters)
                .build())
        .build();
  }
}
