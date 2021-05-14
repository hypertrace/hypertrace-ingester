package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPANS_PER_TRACE_METRIC;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_CREATION_TIME;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.TimestampRecord;
import org.hypertrace.core.datamodel.Timestamps;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.SpanIdentity;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.TraceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks if a trace can be finalized and emitted based on inactivity period of {@link
 * RawSpansProcessor#groupingWindowTimeoutMs}
 */
class TraceEmitPunctuator implements Punctuator {

  private static final Logger logger = LoggerFactory.getLogger(TraceEmitPunctuator.class);

  private static final Timer spansGrouperArrivalLagTimer =
      PlatformMetricsRegistry.registerTimer(DataflowMetricUtils.ARRIVAL_LAG, new HashMap<>());
  private static final Object mutex = new Object();
  private static final String PUNCTUATE_LATENCY_TIMER =
      "hypertrace.rawspansgrouper.punctuate.latency";
  private static final ConcurrentMap<String, Timer> tenantToPunctuateLatencyTimer =
      new ConcurrentHashMap<>();

  private final double dataflowSamplingPercent;
  private final TraceIdentity key;
  private final ProcessorContext context;
  private final KeyValueStore<SpanIdentity, RawSpan> spanStore;
  private final KeyValueStore<TraceIdentity, TraceState> traceStateStore;
  private final To outputTopicProducer;
  private final long groupingWindowTimeoutMs;
  private Cancellable cancellable;

  private static final String TRACES_EMITTER_COUNTER = "hypertrace.emitted.traces";
  private static final ConcurrentMap<String, Counter> tenantToTraceEmittedCounter =
      new ConcurrentHashMap<>();

  TraceEmitPunctuator(
      TraceIdentity key,
      ProcessorContext context,
      KeyValueStore<SpanIdentity, RawSpan> spanStore,
      KeyValueStore<TraceIdentity, TraceState> traceStateStore,
      To outputTopicProducer,
      long groupingWindowTimeoutMs,
      double dataflowSamplingPercent) {
    this.key = key;
    this.context = context;
    this.spanStore = spanStore;
    this.traceStateStore = traceStateStore;
    this.outputTopicProducer = outputTopicProducer;
    this.groupingWindowTimeoutMs = groupingWindowTimeoutMs;
    this.dataflowSamplingPercent = dataflowSamplingPercent;
  }

  public void setCancellable(Cancellable cancellable) {
    this.cancellable = cancellable;
  }

  /** @param timestamp correspond to current system time */
  @Override
  public void punctuate(long timestamp) {
    Instant startTime = Instant.now();
    // always cancel the punctuator else it will get re-scheduled automatically
    cancellable.cancel();

    TraceState traceState = traceStateStore.get(key);
    if (null == traceState
        || null == traceState.getSpanIds()
        || traceState.getSpanIds().isEmpty()) {
      /*
       todo - debug why this happens .
       Typically seen when punctuators are created via {@link RawSpansGroupingTransformer.restorePunctuators}
      */
      logger.warn(
          "TraceState for tenant_id=[{}], trace_id=[{}] is missing.",
          key.getTenantId(),
          HexUtils.getHex(key.getTraceId()));
      return;
    }

    long emitTs = traceState.getEmitTs();
    if (emitTs <= timestamp) {
      // we can emit this trace so just delete the entry for this 'key'
      // Implies that no new spans for the trace have arrived within the last
      // 'groupingWindowTimeoutMs' interval
      // so the trace can be finalized and emitted
      traceStateStore.delete(key);

      ByteBuffer traceId = traceState.getTraceId();
      String tenantId = traceState.getTenantId();
      List<RawSpan> rawSpanList = new ArrayList<>();

      for (ByteBuffer spanId : traceState.getSpanIds()) {
        SpanIdentity spanIdentity = new SpanIdentity(tenantId, traceId, spanId);
        RawSpan rawSpan = spanStore.get(spanIdentity);
        if (null != rawSpan) {
          rawSpanList.add(rawSpan);
          spanStore.delete(spanIdentity);
        }
      }

      if (rawSpanList.size() != traceState.getSpanIds().size()) {
        logger.info("Fetched spanIds: [{}] not matching requested spanIds: [{}] for tenant: [{}] trace: [{}]",
            rawSpanList.size(), traceState.getSpanIds().size(), tenantId,
            HexUtils.getHex(traceId));
      }
      recordSpansPerTrace(rawSpanList.size(), List.of(Tag.of("tenant_id", tenantId)));
      Timestamps timestamps =
          trackEndToEndLatencyTimestamps(timestamp, traceState.getTraceStartTimestamp());
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

      tenantToTraceEmittedCounter
          .computeIfAbsent(
              tenantId,
              k ->
                  PlatformMetricsRegistry.registerCounter(
                      TRACES_EMITTER_COUNTER, Map.of("tenantId", k)))
          .increment();

      tenantToPunctuateLatencyTimer
          .computeIfAbsent(
              tenantId,
              k ->
                  PlatformMetricsRegistry.registerTimer(
                      PUNCTUATE_LATENCY_TIMER, Map.of("tenantId", k)))
          .record(Duration.between(startTime, Instant.now()).toMillis(), TimeUnit.MILLISECONDS);
      context.forward(null, trace, outputTopicProducer);
    } else {
      // implies spans for the trace have arrived within the last 'sessionTimeoutMs' interval
      // so the session inactivity window is extended from the last timestamp
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Re-scheduling emit trigger for tenant_id=[{}], trace_id=[{}] to [{}]",
            key.getTenantId(),
            HexUtils.getHex(key.getTraceId()),
            Instant.ofEpochMilli(emitTs + groupingWindowTimeoutMs));
      }
      long newEmitTs = emitTs + groupingWindowTimeoutMs;
      // if current timestamp is ahead of newEmitTs then just add a grace of 100ms and fire it
      long duration = Math.max(100, newEmitTs - timestamp);
      cancellable =
          context.schedule(Duration.ofMillis(duration), PunctuationType.WALL_CLOCK_TIME, this);
    }
  }

  private Timestamps trackEndToEndLatencyTimestamps(
      long currentTimestamp, long firstSpanTimestamp) {
    Timestamps timestamps = null;
    if (!(Math.random() * 100 <= dataflowSamplingPercent)) {
      spansGrouperArrivalLagTimer.record(
          currentTimestamp - firstSpanTimestamp, TimeUnit.MILLISECONDS);
      Map<String, TimestampRecord> records = new HashMap<>();
      records.put(
          DataflowMetricUtils.SPAN_ARRIVAL_TIME,
          new TimestampRecord(DataflowMetricUtils.SPAN_ARRIVAL_TIME, firstSpanTimestamp));
      records.put(TRACE_CREATION_TIME, new TimestampRecord(TRACE_CREATION_TIME, currentTimestamp));
      timestamps = new Timestamps(records);
    }
    return timestamps;
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
