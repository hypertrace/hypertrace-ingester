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
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.RawSpans;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.TimestampRecord;
import org.hypertrace.core.datamodel.Timestamps;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks if a trace can be finalized and emitted based on inactivity period of {@link
 * RawSpansGroupingTransformer#groupingWindowTimeoutMs}
 */
public class TraceEmitPunctuator implements Punctuator {

  private static final Logger logger = LoggerFactory.getLogger(TraceEmitPunctuator.class);

  private double dataflowSamplingPercent;
  private static final Timer spansGrouperArrivalLagTimer =
      PlatformMetricsRegistry.registerTimer(DataflowMetricUtils.ARRIVAL_LAG, new HashMap<>());
  private static final String PUNCTUATE_LATENCY_TIMER =
      "hypertrace.rawspansgrouper.punctuate.latency";
  private static final ConcurrentMap<String, Timer> tenantToPunctuateLatencyTimer =
      new ConcurrentHashMap<>();

  private TraceIdentity key;
  private ProcessorContext context;
  private KeyValueStore<TraceIdentity, ValueAndTimestamp<RawSpans>> inflightTraceStore;
  private KeyValueStore<TraceIdentity, Long> traceEmitTriggerStore;
  private To outputTopicProducer;
  private long groupingWindowTimeoutMs;
  private Cancellable cancellable;
  private static final Object mutex = new Object();

  private static final String TRACES_EMITTER_COUNTER = "hypertrace.emitted.traces";
  private static final ConcurrentMap<String, Counter> tenantToTraceEmittedCounter =
      new ConcurrentHashMap<>();

  public TraceEmitPunctuator(
      TraceIdentity key,
      ProcessorContext context,
      KeyValueStore<TraceIdentity, ValueAndTimestamp<RawSpans>> inflightTraceStore,
      KeyValueStore<TraceIdentity, Long> traceEmitTriggerStore,
      To outputTopicProducer,
      long groupingWindowTimeoutMs,
      double dataflowSamplingPercent) {
    this.key = key;
    this.context = context;
    this.inflightTraceStore = inflightTraceStore;
    this.traceEmitTriggerStore = traceEmitTriggerStore;
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

    Long emitTs = traceEmitTriggerStore.get(key);
    if (null == emitTs) {
      /*
       todo - debug why this happens .
       Typically seen when punctuators are created via {@link RawSpansGroupingTransformer.restorePunctuators}
      */
      logger.warn(
          "emitTs for tenant_id=[{}], trace_id=[{}] is missing.",
          key.getTenantId(),
          HexUtils.getHex(key.getTraceId()));
      return;
    }

    if (emitTs <= timestamp) {
      // we can emit this trace so just delete the entry for this 'key'
      traceEmitTriggerStore.delete(key);
      // Implies that no new spans for the trace have arrived within the last
      // 'groupingWindowTimeoutMs' interval
      // so the trace can be finalized and emitted
      ValueAndTimestamp<RawSpans> agg = inflightTraceStore.delete(key);
      ByteBuffer traceId;
      String tenantId;
      if (agg != null && !agg.value().getRawSpans().isEmpty()) {
        RawSpan firstSpan = agg.value().getRawSpans().get(0);
        traceId = firstSpan.getTraceId();
        tenantId = firstSpan.getCustomerId();

        List<RawSpan> rawSpanList = agg.value().getRawSpans();
        recordSpansPerTrace(rawSpanList.size(), List.of(Tag.of("tenant_id", tenantId)));
        Timestamps timestamps = trackEndToEndLatencyTimestamps(timestamp, agg);
        StructuredTrace trace =
            StructuredTraceBuilder.buildStructuredTraceFromRawSpans(
                rawSpanList, traceId, tenantId, timestamps);

        logger.debug(
            "Emit tenant_id=[{}], trace_id=[{}], spans_count=[{}]",
            tenantId,
            HexUtils.getHex(traceId),
            rawSpanList.size());

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
      }
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
      long timestamp, ValueAndTimestamp<RawSpans> agg) {
    Timestamps timestamps = null;
    if (!agg.value().getRawSpans().isEmpty() && Math.random() * 100 <= dataflowSamplingPercent) {
      long currentTime = timestamp;
      long firstSpanArrivalTime = agg.value().getRawSpans().get(0).getReceivedTimeMillis();
      spansGrouperArrivalLagTimer.record(currentTime - firstSpanArrivalTime, TimeUnit.MILLISECONDS);
      Map<String, TimestampRecord> records = new HashMap<>();
      records.put(
          DataflowMetricUtils.SPAN_ARRIVAL_TIME,
          new TimestampRecord(DataflowMetricUtils.SPAN_ARRIVAL_TIME, firstSpanArrivalTime));
      records.put(TRACE_CREATION_TIME, new TimestampRecord(TRACE_CREATION_TIME, currentTime));
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

  private void reportLatency(String tenantId, Instant startTime) {}
}
