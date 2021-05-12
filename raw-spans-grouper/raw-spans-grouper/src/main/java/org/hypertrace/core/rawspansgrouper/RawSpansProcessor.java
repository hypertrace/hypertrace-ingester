package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DROPPED_SPANS_COUNTER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INFLIGHT_TRACE_MAX_SPAN_COUNT;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_PRODUCER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_WINDOW_STORE;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_STATE_STORE;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRUNCATED_TRACES_COUNTER;

import com.typesafe.config.Config;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
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
 * interval then the {@link RawSpansProcessor#groupingWindowTimeoutMs} will get reset and the trace
 * will get an additional {@link RawSpansProcessor#groupingWindowTimeoutMs} time to accept spans.
 */
public class RawSpansProcessor
    implements Transformer<TraceIdentity, RawSpan, KeyValue<String, StructuredTrace>> {

  private static final Logger logger = LoggerFactory.getLogger(RawSpansProcessor.class);
  private static final String PROCESSING_LATENCY_TIMER =
      "hypertrace.rawspansgrouper.processing.latency";
  private static final ConcurrentMap<String, Timer> tenantToSpansGroupingTimer =
      new ConcurrentHashMap<>();
  private ProcessorContext context;
  private WindowStore<SpanIdentity, RawSpan> spanWindowStore;
  private KeyValueStore<TraceIdentity, TraceState> traceStateStore;
  private long groupingWindowTimeoutMs;
  private To outputTopic;
  private double dataflowSamplingPercent = -1;
  private static final Map<String, Long> maxSpanCountMap = new HashMap<>();

  // counter for number of spans dropped per tenant
  private static final ConcurrentMap<String, Counter> droppedSpansCounter =
      new ConcurrentHashMap<>();

  // counter for number of truncated traces per tenant
  private static final ConcurrentMap<String, Counter> truncatedTracesCounter =
      new ConcurrentHashMap<>();

  // private final TraceStateStoreWrapper traceStateStoreWrapper;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.spanWindowStore =
        (WindowStore<SpanIdentity, RawSpan>) context.getStateStore(SPAN_WINDOW_STORE);
    this.traceStateStore =
        (KeyValueStore<TraceIdentity, TraceState>) context.getStateStore(TRACE_STATE_STORE);
    // this.traceStateStoreWrapper = new TraceStateStoreWrapper();

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
      subConfig.entrySet().stream()
          .forEach(
              (entry) -> {
                maxSpanCountMap.put(entry.getKey(), subConfig.getLong(entry.getKey()));
              });
    }

    this.outputTopic = To.child(OUTPUT_TOPIC_PRODUCER);

    setPunctuator();
    // restorePunctuators();
  }

  public KeyValue<String, StructuredTrace> transform(TraceIdentity key, RawSpan value) {
    Instant start = Instant.now();
    long currentTimeMs = System.currentTimeMillis();

    TraceState traceState = traceStateStore.get(key);
    boolean firstEntry = (traceState == null);

    if (shouldDropSpan(key, traceState)) {
      return null;
    }

    // add the new span to window store
    spanWindowStore.put(
        new SpanIdentity(key.getTenantId(), key.getTraceId(), value.getEvent().getEventId()),
        value,
        currentTimeMs);

    /*
     the trace emit ts is essentially currentTs + groupingWindowTimeoutMs
     i.e. if there is no span added in the next 'groupingWindowTimeoutMs' interval
     then the trace can be finalized and emitted
    */
    long traceEmitTs = currentTimeMs + groupingWindowTimeoutMs;
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Updating trigger_ts=[{}] for for tenant_id=[{}], trace_id=[{}]",
          Instant.ofEpochMilli(traceEmitTs),
          key.getTenantId(),
          HexUtils.getHex(key.getTraceId()));
    }

    if (firstEntry) {
      traceState =
          TraceState.newBuilder()
              .setTraceStartTimestamp(currentTimeMs)
              .setTraceEndTimestamp(currentTimeMs)
              .setEmitTs(traceEmitTs)
              .setTenantId(key.getTenantId())
              .setTraceId(key.getTraceId())
              .setSpanIds(List.of(value.getEvent().getEventId()))
              .build();
      // schedulePunctuator(key);
    } else {
      traceState.getSpanIds().add(value.getEvent().getEventId());
      traceState.setTraceEndTimestamp(currentTimeMs);
      traceState.setEmitTs(traceEmitTs);
    }

    traceStateStore.put(key, traceState);

    tenantToSpansGroupingTimer
        .computeIfAbsent(
            value.getCustomerId(),
            k ->
                PlatformMetricsRegistry.registerTimer(
                    PROCESSING_LATENCY_TIMER, Map.of("tenantId", k)))
        .record(Duration.between(start, Instant.now()).toMillis(), TimeUnit.MILLISECONDS);
    // the punctuator will emit the trace
    return null;
  }

  private boolean shouldDropSpan(TraceIdentity key, TraceState traceState) {
    if (traceState != null
        && maxSpanCountMap.containsKey(key.getTenantId())
        && traceState.getSpanIds().size() >= maxSpanCountMap.get(key.getTenantId())) {
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
      return true;
    }
    // increment the counter when the number of spans reaches the max.span.count limit.
    if (traceState != null
        && maxSpanCountMap.containsKey(key.getTenantId())
        && traceState.getSpanIds().size() == maxSpanCountMap.get(key.getTenantId())) {
      truncatedTracesCounter
          .computeIfAbsent(
              key.getTenantId(),
              k ->
                  PlatformMetricsRegistry.registerCounter(
                      TRUNCATED_TRACES_COUNTER, Map.of("tenantId", k)))
          .increment();
    }
    return false;
  }

  private void setPunctuator() {
    PeriodicPunctuator punctuator =
        new PeriodicPunctuator(
            context,
            spanWindowStore,
            traceStateStore,
            outputTopic,
            groupingWindowTimeoutMs,
            dataflowSamplingPercent);
    Cancellable cancellable =
        context.schedule(
            Duration.ofMillis(groupingWindowTimeoutMs),
            PunctuationType.WALL_CLOCK_TIME,
            punctuator);
    punctuator.setCancellable(cancellable);
    logger.debug(
        "Scheduled a punctuator to emit trace  to run after [{}] ms", groupingWindowTimeoutMs);
  }

  private void schedulePunctuator(TraceIdentity key) {
    TraceEmitPunctuator punctuator =
        new TraceEmitPunctuator(
            key,
            context,
            spanWindowStore,
            traceStateStore,
            outputTopic,
            groupingWindowTimeoutMs,
            dataflowSamplingPercent);
    Cancellable cancellable =
        context.schedule(
            Duration.ofMillis(groupingWindowTimeoutMs),
            PunctuationType.WALL_CLOCK_TIME,
            punctuator);
    punctuator.setCancellable(cancellable);
    logger.debug(
        "Scheduled a punctuator to emit trace for key=[{}] to run after [{}] ms",
        key,
        groupingWindowTimeoutMs);
  }

  @Override
  public void close() {}

  /**
   * Punctuators are not persisted - so on restart we recover punctuators and schedule them to run
   * after {@link RawSpansProcessor#groupingWindowTimeoutMs}
   */

  /**
   * void restorePunctuators() { long count = 0; Instant start = Instant.now(); try
   * (KeyValueIterator<TraceIdentity, TraceState> it = traceStateStore.all()) { while (it.hasNext())
   * { schedulePunctuator(it.next().key); count++; } logger.info( "Restored=[{}] punctuators,
   * Duration=[{}]", count, Duration.between(start, Instant.now())); } }
   */
}
