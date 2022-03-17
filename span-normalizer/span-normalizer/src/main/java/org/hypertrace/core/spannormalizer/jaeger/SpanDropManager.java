package org.hypertrace.core.spannormalizer.jaeger;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.Timestamps;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class SpanDropManager {
  private final TenantIdHandler tenantIdHandler;
  private final SpanFilter spanFilter;
  private final ExcludeSpanRuleEvaluator excludeSpanRuleEvaluator;
  private static final String LATE_ARRIVAL_THRESHOLD_CONFIG_KEY =
      "processor.late.arrival.threshold.duration";
  private static final ConcurrentMap<String, Counter> tenantToSpansDroppedCount =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Counter> tenantToLateArrivalSpansDroppedCount =
      new ConcurrentHashMap<>();
  private static final String DROPPED_SPANS_COUNTER = "hypertrace.reported.spans.dropped";
  private static final String IS_LATE_ARRIVAL_SPANS_TAGS = "is_late_arrival_spans";
  // list of tenant ids to exclude
  private static final String TENANT_IDS_TO_EXCLUDE_CONFIG = "processor.excludeTenantIds";

  private List<String> tenantIdsToExclude;
  private static final Duration minArrivalThreshold = Duration.of(30, ChronoUnit.SECONDS);
  private final Duration lateArrivalThresholdDuration;

  public SpanDropManager(Config config) {
    tenantIdHandler = new TenantIdHandler(config);
    spanFilter = new SpanFilter(config);
    excludeSpanRuleEvaluator = new ExcludeSpanRuleEvaluator(config);
    lateArrivalThresholdDuration = configureLateArrivalThreshold(config);
    tenantIdsToExclude =
        config.hasPath(TENANT_IDS_TO_EXCLUDE_CONFIG)
            ? config.getStringList(TENANT_IDS_TO_EXCLUDE_CONFIG)
            : Collections.emptyList();
    if (!this.tenantIdsToExclude.isEmpty()) {
      log.info("list of tenant ids to exclude : {}", this.tenantIdsToExclude);
    }
  }

  @VisibleForTesting
  SpanDropManager(Config config, ExcludeSpanRulesCache excludeSpanRulesCache) {
    tenantIdHandler = new TenantIdHandler(config);
    spanFilter = new SpanFilter(config);
    excludeSpanRuleEvaluator = new ExcludeSpanRuleEvaluator(excludeSpanRulesCache);
    lateArrivalThresholdDuration = configureLateArrivalThreshold(config);
    tenantIdsToExclude =
        config.hasPath(TENANT_IDS_TO_EXCLUDE_CONFIG)
            ? config.getStringList(TENANT_IDS_TO_EXCLUDE_CONFIG)
            : Collections.emptyList();
  }

  private Duration configureLateArrivalThreshold(Config jobConfig) {
    Duration configuredThreshold = jobConfig.getDuration(LATE_ARRIVAL_THRESHOLD_CONFIG_KEY);
    if (minArrivalThreshold.compareTo(configuredThreshold) > 0) {
      throw new IllegalArgumentException(
          "the value of " + "processor.late.arrival.threshold.duration should be higher than 30s");
    }
    return configuredThreshold;
  }

  public boolean shouldDropSpan(JaegerSpanInternalModel.Span span, String tenantId) {

    Map<String, JaegerSpanInternalModel.KeyValue> spanTags =
        span.getTagsList().stream()
            .collect(Collectors.toMap(t -> t.getKey().toLowerCase(), t -> t, (v1, v2) -> v2));
    Map<String, JaegerSpanInternalModel.KeyValue> processTags =
        span.getProcess().getTagsList().stream()
            .collect(Collectors.toMap(t -> t.getKey().toLowerCase(), t -> t, (v1, v2) -> v2));

    // TODO: Eventually get rid of span filter and tenantID based filter
    return shouldDropSpansBasedOnTenantIdFilter(tenantId)
        || shouldDropSpansBasedOnSpanFilter(tenantId, span, spanTags, processTags)
        || shouldDropSpansBasedOnExcludeRules(tenantId, span, spanTags, processTags)
        || shouldDropSpansBasedOnLateArrival(tenantId, span);
  }

  public boolean shouldDropEvent(Event event, String tenantId) {
    if (excludeSpanRuleEvaluator.shouldDropEvent(event, tenantId)) {
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

  private boolean shouldDropSpansBasedOnTenantIdFilter(String tenantId) {
    return this.tenantIdsToExclude.contains(tenantId);
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
