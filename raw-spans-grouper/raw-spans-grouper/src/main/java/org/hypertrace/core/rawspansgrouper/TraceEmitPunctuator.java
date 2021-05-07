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
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
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

  private final double dataflowSamplingPercent;
  private final TraceIdentity key;
  private final ProcessorContext context;
  private final WindowStore<SpanIdentity, RawSpan> spanWindowStore;
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
      WindowStore<SpanIdentity, RawSpan> spanWindowStore,
      KeyValueStore<TraceIdentity, TraceState> traceStateStore,
      To outputTopicProducer,
      long groupingWindowTimeoutMs,
      double dataflowSamplingPercent) {
    this.key = key;
    this.context = context;
    this.spanWindowStore = spanWindowStore;
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

      TreeSet<ByteBuffer> spanIdsSet =
          new TreeSet<>(
              (spanId1, spanId2) -> {
                // the sort order for this set is determined by comparing the corresponding {@code
                // SpanIdentifier}
                Serde<SpanIdentity> serde = (Serde<SpanIdentity>) context.keySerde();
                byte[] spanIdentityBytes1 =
                    serde.serializer().serialize("", new SpanIdentity(tenantId, traceId, spanId1));
                byte[] spanIdentityBytes2 =
                    serde.serializer().serialize("", new SpanIdentity(tenantId, traceId, spanId2));
                return Bytes.BYTES_LEXICO_COMPARATOR.compare(
                    spanIdentityBytes1, spanIdentityBytes2);
              });
      spanIdsSet.addAll(traceState.getSpanIds());

      List<RawSpan> rawSpanList = new ArrayList<>();
      try (KeyValueIterator<Windowed<SpanIdentity>, RawSpan> iterator =
          spanWindowStore.fetch(
              new SpanIdentity(tenantId, traceId, spanIdsSet.first()),
              new SpanIdentity(tenantId, traceId, spanIdsSet.last()),
              Instant.ofEpochMilli(traceState.getTraceStartTimestamp()),
              Instant.ofEpochMilli(traceState.getTraceEndTimestamp()))) {
        iterator.forEachRemaining(
            keyValue -> {
              // range search could return extra span ids as well, filter them
              // one scenario when this could occur is when some spanIdentifier for another trace
              // has byte value which is within the {@code spanIdsSet.first()} & {@code
              // spanIdsSet.last()}
              // and was received in the same time range
              if (spanIdsSet.contains(keyValue.value.getEvent().getEventId())) {
                rawSpanList.add(keyValue.value);
              }
            });
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
