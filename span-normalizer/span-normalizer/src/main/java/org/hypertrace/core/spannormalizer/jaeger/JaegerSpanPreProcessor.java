package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.SPAN_NORMALIZER_JOB_CONFIG;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.Counter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.client.ConfigServiceClient;
import org.hypertrace.core.spannormalizer.config.ConfigServiceConfig;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanPreProcessor
    implements Transformer<byte[], Span, KeyValue<byte[], PreProcessedSpan>> {

  static final String SPANS_COUNTER = "hypertrace.reported.spans";
  private static final String DROPPED_SPANS_COUNTER = "hypertrace.reported.spans.dropped";
  private static final Logger LOG = LoggerFactory.getLogger(JaegerSpanPreProcessor.class);
  private static final ConcurrentMap<String, Counter> statusToSpansCounter =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Counter> tenantToSpansDroppedCount =
      new ConcurrentHashMap<>();
  private TenantIdHandler tenantIdHandler;
  private SpanFilter spanFilter;
  private ExcludeSpanRuleEvaluator excludeSpanRuleEvaluator;
  private ConfigServiceClient configServiceClient;

  public JaegerSpanPreProcessor() {
    // empty constructor
  }

  // constructor for testing
  JaegerSpanPreProcessor(Config jobConfig, ConfigServiceClient client) {
    tenantIdHandler = new TenantIdHandler(jobConfig);
    spanFilter = new SpanFilter(jobConfig);
    excludeSpanRuleEvaluator = new ExcludeSpanRuleEvaluator();
    configServiceClient = client;
  }

  @Override
  public void init(ProcessorContext context) {
    Config jobConfig = (Config) context.appConfigs().get(SPAN_NORMALIZER_JOB_CONFIG);
    tenantIdHandler = new TenantIdHandler(jobConfig);
    spanFilter = new SpanFilter(jobConfig);
    excludeSpanRuleEvaluator = new ExcludeSpanRuleEvaluator();
    configServiceClient =
        new ConfigServiceClient(new ConfigServiceConfig(jobConfig), new GrpcChannelRegistry());
  }

  @Override
  public KeyValue<byte[], PreProcessedSpan> transform(byte[] key, Span value) {
    try {
      // this is total spans count received. Irrespective of the fact we are able to parse them, or
      // they have tenantId or not.
      statusToSpansCounter
          .computeIfAbsent(
              "received",
              k -> PlatformMetricsRegistry.registerCounter(SPANS_COUNTER, Map.of("result", k)))
          .increment();

      PreProcessedSpan preProcessedSpan = preProcessSpan(value);

      if (null == preProcessedSpan) {
        statusToSpansCounter
            .computeIfAbsent(
                "dropped",
                k -> PlatformMetricsRegistry.registerCounter(SPANS_COUNTER, Map.of("result", k)))
            .increment();
        return null;
      }

      return new KeyValue<>(key, preProcessedSpan);
    } catch (Exception e) {
      LOG.error("Error preprocessing span", e);
      statusToSpansCounter
          .computeIfAbsent(
              "error",
              k -> PlatformMetricsRegistry.registerCounter(SPANS_COUNTER, Map.of("result", k)))
          .increment();
      return null;
    }
  }

  @VisibleForTesting
  PreProcessedSpan preProcessSpan(Span span) {
    Map<String, JaegerSpanInternalModel.KeyValue> spanTags =
        span.getTagsList().stream()
            .collect(Collectors.toMap(t -> t.getKey().toLowerCase(), t -> t, (v1, v2) -> v2));
    Map<String, JaegerSpanInternalModel.KeyValue> processTags =
        span.getProcess().getTagsList().stream()
            .collect(Collectors.toMap(t -> t.getKey().toLowerCase(), t -> t, (v1, v2) -> v2));

    Optional<String> maybeTenantId =
        tenantIdHandler.getAllowedTenantId(span, spanTags, processTags);
    if (maybeTenantId.isEmpty()) {
      return null;
    }

    String tenantId = maybeTenantId.get();
    List<ExcludeSpanRule> excludeSpanRules;
    try {
      excludeSpanRules =
          configServiceClient
              .getAllExcludeSpanRules(RequestContext.forTenantId(tenantId))
              .getRulesList();
    } catch (Exception e) {
      excludeSpanRules = Collections.emptyList();
    }

    if (spanFilter.shouldDropSpan(span, spanTags, processTags)
        || excludeSpanRuleEvaluator.shouldDropSpan(
            tenantIdHandler.getTenantIdProvider().getTenantIdTagKey(),
            span,
            excludeSpanRules,
            spanTags,
            processTags)) {
      // increment dropped counter at tenant level
      tenantToSpansDroppedCount
          .computeIfAbsent(
              tenantId,
              tenant ->
                  PlatformMetricsRegistry.registerCounter(
                      DROPPED_SPANS_COUNTER, Map.of("tenantId", tenantId)))
          .increment();
      return null;
    }

    return new PreProcessedSpan(tenantId, span);
  }

  @Override
  public void close() {
    // noop
  }
}
