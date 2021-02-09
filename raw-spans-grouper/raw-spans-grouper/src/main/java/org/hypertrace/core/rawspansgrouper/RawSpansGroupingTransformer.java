package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DROPPED_SPANS_COUNTER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INFLIGHT_TRACE_MAX_SPAN_COUNT;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INFLIGHT_TRACE_STORE;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_PRODUCER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_EMIT_TRIGGER_STORE;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRUNCATED_TRACES_COUNTER;

import com.typesafe.config.Config;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.micrometer.core.instrument.Counter;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.RawSpans;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives spans keyed by trace_id and stores them. A {@link TraceEmitPunctuator} is scheduled to
 * run after the {@link RawSpansGroupingTransformer#groupingWindowTimeoutMs} interval to emit the
 * trace. If any spans for the trace arrive within the {@link RawSpansGroupingTransformer#groupingWindowTimeoutMs}
 * interval then the {@link RawSpansGroupingTransformer#groupingWindowTimeoutMs} will get reset and
 * the trace will get an additional {@link RawSpansGroupingTransformer#groupingWindowTimeoutMs} time
 * to accept spans.
 */
public class RawSpansGroupingTransformer implements
    Transformer<TraceIdentity, RawSpan, KeyValue<String, StructuredTrace>> {

  private static final Logger logger = LoggerFactory.getLogger(RawSpansGroupingTransformer.class);
  private ProcessorContext context;
  private KeyValueStore<TraceIdentity, ValueAndTimestamp<RawSpans>> inflightTraceStore;
  private KeyValueStore<TraceIdentity, Long> traceEmitTriggerStore;
  private long groupingWindowTimeoutMs;
  private To outputTopic;
  private double dataflowSamplingPercent = -1;
  private Map<String, Long> maxSpanCountMap = new HashMap<>();

  // counter for number of spans dropped per tenant
  private static final ConcurrentMap<String, Counter> droppedSpansCounter = new ConcurrentHashMap<>();

  // counter for number of truncated traces per tenant
  private static final ConcurrentMap<String, Counter> truncatedTracesCounter = new ConcurrentHashMap<>();

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.inflightTraceStore = (KeyValueStore<TraceIdentity, ValueAndTimestamp<RawSpans>>) context
        .getStateStore(INFLIGHT_TRACE_STORE);
    this.traceEmitTriggerStore = (KeyValueStore<TraceIdentity, Long>) context
        .getStateStore(TRACE_EMIT_TRIGGER_STORE);
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
      subConfig.entrySet().stream().forEach((entry) -> {
        maxSpanCountMap.put(entry.getKey(), subConfig.getLong(entry.getKey()));
      });
    }

    this.outputTopic = To.child(OUTPUT_TOPIC_PRODUCER);
    restorePunctuators();
  }

  @Override
  public KeyValue<String, StructuredTrace> transform(TraceIdentity key, RawSpan value) {
    ValueAndTimestamp<RawSpans> rawSpans = inflightTraceStore.get(key);
    RawSpans agg = rawSpans != null ? rawSpans.value() : RawSpans.newBuilder().build();
    if (maxSpanCountMap.containsKey(key.getTenantId()) && agg.getRawSpans().size() >= maxSpanCountMap.get(key.getTenantId())) {
      if (logger.isDebugEnabled()) {
        logger.debug("Dropping span [{}] from tenant_id={}, trace_id={} after grouping {} spans",
          value,
          key.getTenantId(),
          HexUtils.getHex(key.getTraceId()),
          agg.getRawSpans().size());
      }
      // increment the counter for dropped spans
      droppedSpansCounter.computeIfAbsent(key.getTenantId(),
        k -> PlatformMetricsRegistry.registerCounter(DROPPED_SPANS_COUNTER, Map.of("tenantId", k))).increment();
      return null;
    }

    // add the new span
    agg.getRawSpans().add(value);

    // increment the counter when the trace size reaches the max.span.count limit.
    if (maxSpanCountMap.containsKey(key.getTenantId()) && agg.getRawSpans().size() == maxSpanCountMap.get(key.getTenantId())) {
      truncatedTracesCounter.computeIfAbsent(key.getTenantId(),
        k -> PlatformMetricsRegistry.registerCounter(TRUNCATED_TRACES_COUNTER, Map.of("tenantId", k))).increment();
    }

    long currentTimeMs = System.currentTimeMillis();

    if (logger.isDebugEnabled()) {
      logger.debug("Updating ts=[{}] for tenant_id=[{}], trace_id=[{}]",
          Instant.ofEpochMilli(currentTimeMs), key.getTenantId(),
          HexUtils.getHex(key.getTraceId()));
    }
    // store the current system time as the last update timestamp
    inflightTraceStore.put(key, ValueAndTimestamp.make(agg, currentTimeMs));

    /*
     check if any prior trace emit ts was stored. It is stored when an entry for a trace doesn't exist.
     See the {@link TraceEmitPunctuator} for details on how the traceEmitTs is updated
    */
    Long triggerTs = traceEmitTriggerStore.get(key);
    boolean firstEntry = (triggerTs == null);
    /*
     the trace emit ts is essentially currentTs + groupingWindowTimeoutMs
     i.e. if there is no span added in the next 'groupingWindowTimeoutMs' interval
     then the trace can be finalized and emitted
    */

    long traceEmitTs = currentTimeMs + groupingWindowTimeoutMs;
    if (logger.isDebugEnabled()) {
      logger.debug("Updating trigger_ts=[{}] for for tenant_id=[{}], trace_id=[{}]",
          Instant.ofEpochMilli(traceEmitTs),
          key.getTenantId(),
          HexUtils.getHex(key.getTraceId()));
    }
    traceEmitTriggerStore.put(key, traceEmitTs);

    // if no prior emitTs is found then add it.
    if (firstEntry) {
      schedulePunctuator(key);
    }
    // the punctuator will emit the trace
    return null;
  }

  private void schedulePunctuator(TraceIdentity key) {
    TraceEmitPunctuator punctuator = new TraceEmitPunctuator(key, context, inflightTraceStore,
        traceEmitTriggerStore,
        outputTopic, groupingWindowTimeoutMs, dataflowSamplingPercent);
    Cancellable cancellable = context
        .schedule(Duration.ofMillis(groupingWindowTimeoutMs), PunctuationType.WALL_CLOCK_TIME,
            punctuator);
    punctuator.setCancellable(cancellable);
    logger.debug("Scheduled a punctuator to emit trace for key=[{}] to run after [{}] ms", key,
        groupingWindowTimeoutMs);
  }

  @Override
  public void close() {
  }

  /**
   * Punctuators are not persisted - so on restart we recover punctuators and schedule them to run
   * after {@link RawSpansGroupingTransformer#groupingWindowTimeoutMs}
   */
  void restorePunctuators() {
    long count = 0;
    Instant start = Instant.now();
    try (KeyValueIterator<TraceIdentity, Long> it = traceEmitTriggerStore.all()) {
      while (it.hasNext()) {
        schedulePunctuator(it.next().key);
        count++;
      }
      logger.info("Restored=[{}] punctuators, Duration=[{}]", count,
          Duration.between(start, Instant.now()));
    }
  }
}
