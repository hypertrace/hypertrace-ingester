package org.hypertrace.core.spannormalizer.jaeger;

import com.google.protobuf.util.Timestamps;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

public class SpanDropManager {
  private TenantIdHandler tenantIdHandler;
  private SpanFilter spanFilter;
  private ExcludeSpanRuleEvaluator excludeSpanRuleEvaluator;
  private static final String LATE_ARRIVAL_THRESHOLD_CONFIG_KEY =
      "processor.late.arrival.threshold.duration";
  private static final ConcurrentMap<String, Counter> tenantToSpansDroppedCount =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Counter> tenantToLateArrivalSpansDroppedCount =
      new ConcurrentHashMap<>();
  private static final String DROPPED_SPANS_COUNTER = "hypertrace.reported.spans.dropped";
  private static final String IS_LATE_ARRIVAL_SPANS_TAGS = "is_late_arrival_spans";
  private static final Duration minArrivalThreshold = Duration.of(30, ChronoUnit.SECONDS);
  private Duration lateArrivalThresholdDuration;

  public SpanDropManager(Config config) {
    tenantIdHandler = new TenantIdHandler(config);
    spanFilter = new SpanFilter(config);
    excludeSpanRuleEvaluator =
        new ExcludeSpanRuleEvaluator(ExcludeSpanRuleCache.getInstance(config));
    lateArrivalThresholdDuration = configureLateArrivalThreshold(config);
  }

  // for testing
  SpanDropManager(Config config, ExcludeSpanRuleCache excludeSpanRuleCache) {
    tenantIdHandler = new TenantIdHandler(config);
    spanFilter = new SpanFilter(config);
    excludeSpanRuleEvaluator = new ExcludeSpanRuleEvaluator(excludeSpanRuleCache);
    lateArrivalThresholdDuration = configureLateArrivalThreshold(config);
  }

  private Duration configureLateArrivalThreshold(Config jobConfig) {
    Duration configuredThreshold = jobConfig.getDuration(LATE_ARRIVAL_THRESHOLD_CONFIG_KEY);
    if (minArrivalThreshold.compareTo(configuredThreshold) > 0) {
      throw new IllegalArgumentException(
          "the value of " + "processor.late.arrival.threshold.duration should be higher than 30s");
    }
    return configuredThreshold;
  }

  public boolean shouldDropSpan(
      JaegerSpanInternalModel.Span span,
      Map<String, JaegerSpanInternalModel.KeyValue> spanTags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags) {

    // TODO: eventually get rid of this tenantId based filter
    Optional<String> maybeTenantId =
        tenantIdHandler.getAllowedTenantId(span, spanTags, processTags);
    if (maybeTenantId.isEmpty()) {
      return true;
    }

    String tenantId = maybeTenantId.get();
    // TODO: Eventually get rid of span filter
    return shouldDropSpansBasedOnSpanFilter(tenantId, span, spanTags, processTags)
        || shouldDropSpansBasedOnExcludeRules(tenantId, span, spanTags, processTags)
        || shouldDropSpansBasedOnLateArrival(tenantId, span);
  }

  private boolean shouldDropSpansBasedOnSpanFilter(
      String tenantId,
      JaegerSpanInternalModel.Span span,
      Map<String, JaegerSpanInternalModel.KeyValue> spanTags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags) {
    if (spanFilter.shouldDropSpan(span, spanTags, processTags)) {
      // increment dropped counter at tenant level
      tenantToSpansDroppedCount
          .computeIfAbsent(
              tenantId,
              tenant ->
                  PlatformMetricsRegistry.registerCounter(
                      DROPPED_SPANS_COUNTER, Map.of("tenantId", tenantId)))
          .increment();
      return true;
    }
    return false;
  }

  private boolean shouldDropSpansBasedOnExcludeRules(
      String tenantId,
      JaegerSpanInternalModel.Span span,
      Map<String, JaegerSpanInternalModel.KeyValue> spanTags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags) {
    if (excludeSpanRuleEvaluator.shouldDropSpan(
        tenantIdHandler.getTenantIdProvider().getTenantIdTagKey(),
        tenantId,
        span,
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
      return true;
    }
    return false;
  }

  private boolean shouldDropSpansBasedOnLateArrival(
      String tenantId, JaegerSpanInternalModel.Span span) {
    long spanProcessedTime = System.currentTimeMillis();
    long spanStartTime = Timestamps.toMillis(span.getStartTime());
    Duration spanArrivalDelay =
        Duration.of(Math.abs(spanProcessedTime - spanStartTime), ChronoUnit.MILLIS);

    if (spanStartTime > 0 && spanArrivalDelay.compareTo(lateArrivalThresholdDuration) > 0) {
      tenantToLateArrivalSpansDroppedCount
          .computeIfAbsent(
              tenantId,
              tenant ->
                  PlatformMetricsRegistry.registerCounter(
                      DROPPED_SPANS_COUNTER,
                      Map.of("tenantId", tenantId, IS_LATE_ARRIVAL_SPANS_TAGS, "true")))
          .increment();
      return true;
    }
    return false;
  }
}
