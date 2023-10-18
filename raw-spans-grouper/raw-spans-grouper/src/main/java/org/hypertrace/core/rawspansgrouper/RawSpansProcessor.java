package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
import static org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DEFAULT_INFLIGHT_TRACE_MAX_SPAN_COUNT;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DROPPED_SPANS_COUNTER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INFLIGHT_TRACE_MAX_SPAN_COUNT;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_PRODUCER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_STATE_STORE_NAME;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_EMIT_PUNCTUATOR;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_EMIT_PUNCTUATOR_FREQUENCY_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_EMIT_PUNCTUATOR_STORE_NAME;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_STATE_STORE;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRUNCATED_TRACES_COUNTER;

import com.typesafe.config.Config;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.kafkastreams.framework.punctuators.ThrottledPunctuatorConfig;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.SpanIdentity;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.TraceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives spans keyed by trace_id and stores them. A {@link TraceEmitPunctuator} is scheduled to
 * run after the {@link RawSpansProcessor#groupingWindowTimeoutMs} interval to emit the trace. If
 * any spans for the trace arrive within the {@link RawSpansProcessor#groupingWindowTimeoutMs}
 * interval then the trace will get an additional {@link RawSpansProcessor#groupingWindowTimeoutMs}
 * time to accept spans.
 */
public class RawSpansProcessor
    implements Processor<TraceIdentity, RawSpan, TraceIdentity, StructuredTrace> {

  private static final Logger logger = LoggerFactory.getLogger(RawSpansProcessor.class);
  private static final String PROCESSING_LATENCY_TIMER =
      "hypertrace.rawspansgrouper.processing.latency";
  private static final ConcurrentMap<String, Timer> tenantToSpansGroupingTimer =
      new ConcurrentHashMap<>();
  // counter for number of spans dropped per tenant
  private static final ConcurrentMap<String, Counter> droppedSpansCounter =
      new ConcurrentHashMap<>();

  // counter for number of truncated traces per tenant
  private static final ConcurrentMap<String, Counter> truncatedTracesCounter =
      new ConcurrentHashMap<>();
  private final Clock clock;
  private KeyValueStore<SpanIdentity, RawSpan> spanStore;
  private KeyValueStore<TraceIdentity, TraceState> traceStateStore;
  private long groupingWindowTimeoutMs;
  private double dataflowSamplingPercent = -1;
  private static final Map<String, Long> maxSpanCountMap = new HashMap<>();
  private long defaultMaxSpanCountLimit = Long.MAX_VALUE;
  private TraceEmitPunctuator traceEmitPunctuator;
  private Cancellable traceEmitTasksPunctuatorCancellable;

  public RawSpansProcessor(Clock clock) {
    this.clock = clock;
  }

  @Override
  public void init(ProcessorContext<TraceIdentity, StructuredTrace> context) {
    this.spanStore = context.getStateStore(SPAN_STATE_STORE_NAME);
    this.traceStateStore = context.getStateStore(TRACE_STATE_STORE);
    Config jobConfig = (Config) (context.appConfigs().get(RAW_SPANS_GROUPER_JOB_CONFIG));
    this.groupingWindowTimeoutMs =
        jobConfig.getLong(SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY) * 1000;

    if (jobConfig.hasPath(DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY)
        && jobConfig.getDouble(DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY) > 0
        && jobConfig.getDouble(DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY) <= 100) {
      this.dataflowSamplingPercent = jobConfig.getDouble(DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY);
    }

    if (jobConfig.hasPath(INFLIGHT_TRACE_MAX_SPAN_COUNT)) {
      Config subConfig = jobConfig.getConfig(INFLIGHT_TRACE_MAX_SPAN_COUNT);
      subConfig
          .entrySet()
          .forEach(
              (entry) -> {
                maxSpanCountMap.put(entry.getKey(), subConfig.getLong(entry.getKey()));
              });
    }

    if (jobConfig.hasPath(DEFAULT_INFLIGHT_TRACE_MAX_SPAN_COUNT)) {
      defaultMaxSpanCountLimit = jobConfig.getLong(DEFAULT_INFLIGHT_TRACE_MAX_SPAN_COUNT);
    }

    KeyValueStore<Long, List<TraceIdentity>> traceEmitPunctuatorStore =
        context.getStateStore(TRACE_EMIT_PUNCTUATOR_STORE_NAME);
    traceEmitPunctuator =
        new TraceEmitPunctuator(
            new ThrottledPunctuatorConfig(
                jobConfig.getConfig(KAFKA_STREAMS_CONFIG_KEY), TRACE_EMIT_PUNCTUATOR),
            traceEmitPunctuatorStore,
            context,
            spanStore,
            traceStateStore,
            OUTPUT_TOPIC_PRODUCER,
            groupingWindowTimeoutMs,
            dataflowSamplingPercent);
    // Punctuator scheduled on stream time => no input messages => no emits will happen
    // We will almost never have input down to 0, i.e., there are no spans coming to platform,
    // While using wall clock time handles that case, there is an issue with using wall clock.
    // In cases of lag being burnt, we are processing message produced at different time stamp
    // intervals, probably at higher rate than which they were produced, now not doing punctuation
    // often will increase the amount of work yielding punctuator in next iterations and will keep
    // on piling up until lag is burnt completely and only then the punctuator will catch up back to
    // normal input rate. This is undesirable, here the outputs are only emitted from punctuator.
    // If we burn lag from input topic, we want to push it down to output & downstream as soon
    // as possible, if we hog it more and more it will delay cascading lag to downstream. Given
    // grouper stays at start of pipeline and also that input dying down almost never happens
    // it is better to use stream time over wall clock time for yielding trace emit tasks punctuator
    traceEmitTasksPunctuatorCancellable =
        context.schedule(
            jobConfig.getDuration(TRACE_EMIT_PUNCTUATOR_FREQUENCY_CONFIG_KEY),
            PunctuationType.STREAM_TIME,
            traceEmitPunctuator);
  }

  @Override
  public void process(Record<TraceIdentity, RawSpan> record) {
    Instant start = Instant.now();
    long currentTimeMs = clock.millis();

    TraceIdentity key = record.key();
    RawSpan value = record.value();
    TraceState traceState = traceStateStore.get(key);
    boolean firstEntry = (traceState == null);

    if (shouldDropSpan(key, traceState)) {
      return;
    }

    String tenantId = key.getTenantId();
    ByteBuffer traceId = value.getTraceId();
    ByteBuffer spanId = value.getEvent().getEventId();
    spanStore.put(new SpanIdentity(tenantId, traceId, spanId), value);

    if (firstEntry) {
      traceState =
          fastNewBuilder(TraceState.Builder.class)
              .setTraceStartTimestamp(currentTimeMs)
              .setTraceEndTimestamp(currentTimeMs)
              .setEmitTs(-1) // deprecated, not used anymore
              .setTenantId(tenantId)
              .setTraceId(traceId)
              .setSpanIds(List.of(spanId))
              .build();
      traceEmitPunctuator.scheduleTask(currentTimeMs, key);
    } else {
      traceState.getSpanIds().add(spanId);
      long prevScheduleTimestamp = traceState.getTraceEndTimestamp() + groupingWindowTimeoutMs;
      traceState.setTraceEndTimestamp(currentTimeMs);
      if (!traceEmitPunctuator.rescheduleTask(
          prevScheduleTimestamp, currentTimeMs + groupingWindowTimeoutMs, key)) {
        logger.debug(
            "Failed to proactively reschedule task on getting span for trace key {}, schedule already dropped!",
            key);
      }
    }

    traceStateStore.put(key, traceState);

    tenantToSpansGroupingTimer
        .computeIfAbsent(
            value.getCustomerId(),
            k ->
                PlatformMetricsRegistry.registerTimer(
                    PROCESSING_LATENCY_TIMER, Map.of("tenantId", k)))
        .record(Duration.between(start, Instant.now()).toMillis(), TimeUnit.MILLISECONDS);
    // no need to do context.forward. the punctuator will emit the trace once it's eligible to be
    // emitted
    return;
  }

  private boolean shouldDropSpan(TraceIdentity key, TraceState traceState) {
    int inFlightSpansPerTrace = traceState != null ? traceState.getSpanIds().size() : 0;
    long maxSpanCountTenantLimit =
        maxSpanCountMap.containsKey(key.getTenantId())
            ? maxSpanCountMap.get(key.getTenantId())
            : defaultMaxSpanCountLimit;

    if (inFlightSpansPerTrace >= maxSpanCountTenantLimit) {

      if (logger.isDebugEnabled()) {
        logger.debug(
            "Dropping span [{}] from tenant_id={}, trace_id={} after grouping {} spans",
            traceState.getSpanIds().stream().map(HexUtils::getHex).collect(Collectors.toList()),
            key.getTenantId(),
            HexUtils.getHex(key.getTraceId()),
            traceState.getSpanIds().size());
      }

      // increment the counter for dropped spans
      droppedSpansCounter
          .computeIfAbsent(
              key.getTenantId(),
              k ->
                  PlatformMetricsRegistry.registerCounter(
                      DROPPED_SPANS_COUNTER, Map.of("tenantId", k)))
          .increment();

      // increment the counter when the number of spans reaches the max.span.count limit.
      if (inFlightSpansPerTrace == maxSpanCountTenantLimit) {
        truncatedTracesCounter
            .computeIfAbsent(
                key.getTenantId(),
                k ->
                    PlatformMetricsRegistry.registerCounter(
                        TRUNCATED_TRACES_COUNTER, Map.of("tenantId", k)))
            .increment();
      }
      // drop the span as limit is reached
      return true;
    }
    return false;
  }

  @Override
  public void close() {
    traceEmitTasksPunctuatorCancellable.cancel();
  }
}
