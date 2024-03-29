package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPANS_PER_TRACE_METRIC;

import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.Timestamps;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.hypertrace.core.kafkastreams.framework.punctuators.AbstractThrottledPunctuator;
import org.hypertrace.core.kafkastreams.framework.punctuators.ThrottledPunctuatorConfig;
import org.hypertrace.core.kafkastreams.framework.punctuators.action.CompletedTaskResult;
import org.hypertrace.core.kafkastreams.framework.punctuators.action.RescheduleTaskResult;
import org.hypertrace.core.kafkastreams.framework.punctuators.action.TaskResult;
import org.hypertrace.core.rawspansgrouper.utils.TraceLatencyMeter;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.SpanIdentity;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.TraceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Check if a trace can be finalized and emitted based on inactivity period of {@link
 * RawSpansProcessor#groupingWindowTimeoutMs}
 */
class TraceEmitPunctuator extends AbstractThrottledPunctuator<TraceIdentity> {

  private static final Logger logger = LoggerFactory.getLogger(TraceEmitPunctuator.class);
  private static final Object mutex = new Object();

  private static final Timer spansGrouperArrivalLagTimer =
      PlatformMetricsRegistry.registerTimer(DataflowMetricUtils.ARRIVAL_LAG, new HashMap<>());
  private static final String TRACES_EMITTER_COUNTER = "hypertrace.emitted.traces";
  private static final ConcurrentMap<String, Counter> tenantToTraceEmittedCounter =
      new ConcurrentHashMap<>();
  private static final String SPANS_PER_TRACE = "hypertrace.rawspansgrouper.spans.per.trace";
  private static final ConcurrentMap<String, Counter> tenantToSpanPerTraceCounter =
      new ConcurrentHashMap<>();
  private static final RateLimiter spanStoreCountRateLimiter = RateLimiter.create(1 / 60d);
  private static final String SPAN_STORE_COUNT = "hypertrace.rawspansgrouper.span.store.count";
  private static final ConcurrentMap<String, Counter> tenantToSpanStoreCountCounter =
      new ConcurrentHashMap<>();
  private static final String TRACE_WITH_DUPLICATE_SPANS =
      "hypertrace.rawspansgrouper.trace.with.duplicate.spans";
  private static final ConcurrentMap<String, Counter> tenantToTraceWithDuplicateSpansCounter =
      new ConcurrentHashMap<>();

  private static final CompletedTaskResult COMPLETED_TASK_RESULT = new CompletedTaskResult();
  private final ProcessorContext<TraceIdentity, StructuredTrace> context;
  private final KeyValueStore<SpanIdentity, RawSpan> spanStore;
  private final KeyValueStore<TraceIdentity, TraceState> traceStateStore;
  private final String outputTopicProducer;
  private final long groupingWindowTimeoutMs;
  private final TraceLatencyMeter traceLatencyMeter;

  TraceEmitPunctuator(
      ThrottledPunctuatorConfig throttledPunctuatorConfig,
      KeyValueStore<Long, List<TraceIdentity>> throttledPunctuatorStore,
      ProcessorContext<TraceIdentity, StructuredTrace> context,
      KeyValueStore<SpanIdentity, RawSpan> spanStore,
      KeyValueStore<TraceIdentity, TraceState> traceStateStore,
      String outputTopicProducer,
      long groupingWindowTimeoutMs,
      double dataflowSamplingPercent) {
    super(Clock.systemUTC(), throttledPunctuatorConfig, throttledPunctuatorStore);
    this.context = context;
    this.spanStore = spanStore;
    this.traceStateStore = traceStateStore;
    this.outputTopicProducer = outputTopicProducer;
    this.groupingWindowTimeoutMs = groupingWindowTimeoutMs;
    this.traceLatencyMeter = new TraceLatencyMeter(dataflowSamplingPercent);
  }

  protected TaskResult executeTask(long punctuateTimestamp, TraceIdentity key) {
    TraceState traceState = traceStateStore.get(key);
    if (null == traceState
        || null == traceState.getSpanIds()
        || traceState.getSpanIds().isEmpty()) {
      logger.warn(
          "TraceState for tenant_id=[{}], trace_id=[{}] is missing.",
          key.getTenantId(),
          HexUtils.getHex(key.getTraceId()));
      return COMPLETED_TASK_RESULT;
    }
    if (punctuateTimestamp - traceState.getTraceEndTimestamp() >= groupingWindowTimeoutMs) {
      // Implies that no new spans for the trace have arrived within the last
      // 'groupingWindowTimeoutMs' interval
      // so the trace can be finalized and emitted
      emitTrace(key, traceState);
      // no need of running again for this
      return COMPLETED_TASK_RESULT;
    }
    return new RescheduleTaskResult(traceState.getTraceEndTimestamp() + groupingWindowTimeoutMs);
  }

  private void emitTrace(TraceIdentity key, TraceState traceState) {
    traceStateStore.delete(key);

    ByteBuffer traceId = traceState.getTraceId();
    String tenantId = traceState.getTenantId();
    List<RawSpan> rawSpanList = new ArrayList<>();

    Set<ByteBuffer> spanIds = new HashSet<>(traceState.getSpanIds());
    spanIds.forEach(
        v -> {
          SpanIdentity spanIdentity = new SpanIdentity(tenantId, traceId, v);
          RawSpan rawSpan = spanStore.delete(spanIdentity);
          // ideally this shouldn't happen
          if (rawSpan != null) {
            rawSpanList.add(rawSpan);
          }
        });

    if (traceState.getSpanIds().size() != spanIds.size()) {
      tenantToTraceWithDuplicateSpansCounter
          .computeIfAbsent(
              tenantId,
              k ->
                  PlatformMetricsRegistry.registerCounter(
                      TRACE_WITH_DUPLICATE_SPANS, Map.of("tenantId", k)))
          .increment();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Duplicate spanIds: [{}], unique spanIds count: [{}] for tenant: [{}] trace: [{}]",
            traceState.getSpanIds().size(),
            spanIds.size(),
            tenantId,
            HexUtils.getHex(traceId));
      }
    }

    recordSpansPerTrace(rawSpanList.size(), List.of(Tag.of("tenant_id", tenantId)));
    Timestamps timestamps =
        this.traceLatencyMeter.trackEndToEndLatencyTimestamps(
            System.currentTimeMillis(), traceState.getTraceStartTimestamp());
    StructuredTrace trace =
        StructuredTraceBuilder.buildStructuredTraceFromRawSpans(
            rawSpanList, traceId, tenantId, timestamps);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Emit tenant_id=[{}], trace_id=[{}], spans_count=[{}]",
          tenantId,
          HexUtils.getHex(traceId),
          rawSpanList.size());
    }

    // report entries in spanStore
    if (spanStoreCountRateLimiter.tryAcquire()) {
      tenantToSpanStoreCountCounter
          .computeIfAbsent(
              tenantId,
              k -> PlatformMetricsRegistry.registerCounter(SPAN_STORE_COUNT, Map.of("tenantId", k)))
          .increment(spanStore.approximateNumEntries() * 1.0);
    }

    // report count of spanIds per trace
    tenantToSpanPerTraceCounter
        .computeIfAbsent(
            tenantId,
            k -> PlatformMetricsRegistry.registerCounter(SPANS_PER_TRACE, Map.of("tenantId", k)))
        .increment(spanIds.size() * 1.0);

    // report trace emitted count
    tenantToTraceEmittedCounter
        .computeIfAbsent(
            tenantId,
            k ->
                PlatformMetricsRegistry.registerCounter(
                    TRACES_EMITTER_COUNTER, Map.of("tenantId", k)))
        .increment();

    context.forward(
        new Record<TraceIdentity, StructuredTrace>(key, trace, System.currentTimeMillis()),
        outputTopicProducer);
  }

  private void recordSpansPerTrace(double count, Iterable<Tag> tags) {
    DistributionSummary summary =
        DistributionSummary.builder(SPANS_PER_TRACE_METRIC)
            .tags(tags)
            .publishPercentiles(.5, .90, .99)
            .register(PlatformMetricsRegistry.getMeterRegistry());
    // For a given name + tags the same Meter object is used and will be shared across StreamThreads
    synchronized (mutex) {
      summary.record(count);
    }
  }
}
