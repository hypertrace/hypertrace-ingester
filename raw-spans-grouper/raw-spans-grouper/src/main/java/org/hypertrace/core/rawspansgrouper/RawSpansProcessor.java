package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DROPPED_SPANS_COUNTER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INFLIGHT_TRACE_MAX_SPAN_COUNT;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_PRODUCER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPANS_CHUNK_STATE_STORE_NAME;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPANS_CHUNK_STORE_SPAN_COUNT;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_STATE_STORE;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRUNCATED_TRACES_COUNTER;

import com.typesafe.config.Config;
import io.micrometer.core.instrument.Counter;
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
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.RawSpansChunk;
import org.hypertrace.core.spannormalizer.RawSpansChunkIdentity;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.TraceState;
import org.hypertrace.core.spannormalizer.TraceStateV2;
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
    implements Transformer<TraceIdentity, byte[], KeyValue<String, StructuredTrace>> {

  private static final Logger logger = LoggerFactory.getLogger(RawSpansProcessor.class);
  private static final String PROCESSING_LATENCY_TIMER =
      "hypertrace.rawspansgrouper.processing.latency";
  private static final ConcurrentMap<String, Timer> tenantToSpansGroupingTimer =
      new ConcurrentHashMap<>();
  private static final Map<String, Long> maxSpanCountMap = new HashMap<>();
  // counter for number of spans dropped per tenant
  private static final ConcurrentMap<String, Counter> droppedSpansCounter =
      new ConcurrentHashMap<>();
  // counter for number of truncated traces per tenant
  private static final ConcurrentMap<String, Counter> truncatedTracesCounter =
      new ConcurrentHashMap<>();

  private ProcessorContext context;
  private KeyValueStore<RawSpansChunkIdentity, RawSpansChunk> spansChunkStore;
  private KeyValueStore<TraceIdentity, TraceStateV2> traceStateStore;
  private long groupingWindowTimeoutMs;
  private To outputTopic;
  private double dataflowSamplingPercent = -1;
  private int spansChunkStoreSpanCount;
  private Serde<RawSpan> rawSpanAvroSerde;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.spansChunkStore =
        (KeyValueStore<RawSpansChunkIdentity, RawSpansChunk>) context.getStateStore(SPANS_CHUNK_STATE_STORE_NAME);
    this.traceStateStore =
        (KeyValueStore<TraceIdentity, TraceStateV2>) context.getStateStore(TRACE_STATE_STORE);
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

    if (jobConfig.hasPath(SPANS_CHUNK_STORE_SPAN_COUNT)) {
      this.spansChunkStoreSpanCount = jobConfig.getInt(SPANS_CHUNK_STATE_STORE_NAME);
    } else {
      this.spansChunkStoreSpanCount = 1;
    }

    rawSpanAvroSerde = (Serde<RawSpan>) context.valueSerde();
    this.outputTopic = To.child(OUTPUT_TOPIC_PRODUCER);
    restorePunctuators();
  }

  public KeyValue<String, StructuredTrace> transform(TraceIdentity key, byte[] rawSpan) {
    Instant start = Instant.now();
    long currentTimeMs = System.currentTimeMillis();

    TraceStateV2 traceState = traceStateStore.get(key);
    boolean firstEntry = (traceState == null);

    if (shouldDropSpan(key, traceState)) {
      return null;
    }

    String tenantId = key.getTenantId();
    ByteBuffer traceId = key.getTraceId();

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
          HexUtils.getHex(traceId));
    }

    if (firstEntry) {
      traceState =
          TraceStateV2.newBuilder()
              .setTraceStartTimestamp(currentTimeMs)
              .setTraceEndTimestamp(currentTimeMs)
              .setEmitTs(traceEmitTs)
              .setTenantId(tenantId)
              .setTraceId(traceId)
              .setLastChunkId(0)
              .setLastChunkSpanCount(1)
              .setChunkIds(List.of(0))
              .build();
      RawSpansChunk rawSpansChunk = RawSpansChunk.newBuilder()
          .setRawSpans(List.of(ByteBuffer.wrap(rawSpan)))
          .build();
      spansChunkStore.put(
          RawSpansChunkIdentity.newBuilder().setChunkId(0).setTenantId(tenantId).setTraceId(traceId).build(),
          rawSpansChunk);
      schedulePunctuator(key);
    } else {
      if (traceState.getLastChunkSpanCount() < spansChunkStoreSpanCount) {
        // add span to existing chunk
        traceState.setLastChunkSpanCount(traceState.getLastChunkSpanCount() + 1);
        RawSpansChunkIdentity rawSpansChunkIdentity = RawSpansChunkIdentity.newBuilder().setChunkId(traceState.getLastChunkId()).setTenantId(tenantId).setTraceId(traceId).build();
        RawSpansChunk rawSpansChunk = spansChunkStore.get(rawSpansChunkIdentity);
        rawSpansChunk.getRawSpans().add(ByteBuffer.wrap(rawSpan));
        spansChunkStore.put(rawSpansChunkIdentity, rawSpansChunk);
      } else {
        // create new chunk
        traceState.setLastChunkSpanCount(1);
        traceState.setLastChunkId(traceState.getLastChunkId() + 1);
        traceState.getChunkIds().add(traceState.getLastChunkId());
        spansChunkStore.put(
            RawSpansChunkIdentity.newBuilder().setChunkId(traceState.getLastChunkId()).setTenantId(tenantId).setTraceId(traceId).build(),
            RawSpansChunk.newBuilder()
                .setRawSpans(List.of(ByteBuffer.wrap(rawSpan)))
                .build());
      }
      traceState.setTraceEndTimestamp(currentTimeMs);
      traceState.setEmitTs(traceEmitTs);
    }

    traceStateStore.put(key, traceState);

    tenantToSpansGroupingTimer
        .computeIfAbsent(
            tenantId,
            k ->
                PlatformMetricsRegistry.registerTimer(
                    PROCESSING_LATENCY_TIMER, Map.of("tenantId", k)))
        .record(Duration.between(start, Instant.now()).toMillis(), TimeUnit.MILLISECONDS);
    // the punctuator will emit the trace
    return null;
  }

  private boolean shouldDropSpan(TraceIdentity key, TraceStateV2 traceState) {
    if (null == traceState) {
      return false;
    }
    int totalSpanInTrace = getTotalSpansInTrace(traceState);
    if (maxSpanCountMap.containsKey(key.getTenantId())
        && totalSpanInTrace >= maxSpanCountMap.get(key.getTenantId())) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Dropping span from tenant_id={}, trace_id={} after grouping {} spans",
            key.getTenantId(),
            HexUtils.getHex(key.getTraceId()),
            totalSpanInTrace);
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
    if (maxSpanCountMap.containsKey(key.getTenantId())
        && getTotalSpansInTrace(traceState) == maxSpanCountMap.get(key.getTenantId())) {
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

  private int getTotalSpansInTrace(TraceStateV2 traceState) {
    return (traceState.getChunkIds().size() - 1) * spansChunkStoreSpanCount + traceState.getLastChunkSpanCount();
  }

  private void schedulePunctuator(TraceIdentity key) {
    TraceEmitPunctuator punctuator =
        new TraceEmitPunctuator(
            key,
            context,
            spansChunkStore,
            traceStateStore,
            outputTopic,
            groupingWindowTimeoutMs,
            dataflowSamplingPercent,
            rawSpanAvroSerde);
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
  void restorePunctuators() {
    long count = 0;
    Instant start = Instant.now();
    try (KeyValueIterator<TraceIdentity, TraceStateV2> it = traceStateStore.all()) {
      while (it.hasNext()) {
        schedulePunctuator(it.next().key);
        count++;
      }
      logger.info(
          "Restored=[{}] punctuators, Duration=[{}]",
          count,
          Duration.between(start, Instant.now()));
    }
  }
}
