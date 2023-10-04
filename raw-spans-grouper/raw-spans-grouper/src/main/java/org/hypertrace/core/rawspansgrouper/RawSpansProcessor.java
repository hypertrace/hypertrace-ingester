package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DEFAULT_INFLIGHT_TRACE_MAX_SPAN_COUNT;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.DROPPED_SPANS_COUNTER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INFLIGHT_TRACE_MAX_SPAN_COUNT;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_PRODUCER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_STATE_STORE_NAME;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_EMIT_CALLBACK_REGISTRY_FREQUENCY_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_EMIT_CALLBACK_REGISTRY_STORE_NAME;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_EMIT_CALLBACK_REGISTRY_WINDOW_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_EMIT_CALLBACK_REGISTRY_YIELD_CONFIG_KEY;
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.kafkastreams.framework.callbacks.CallbackRegistryPunctuator;
import org.hypertrace.core.kafkastreams.framework.callbacks.CallbackRegistryPunctuatorConfig;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.SpanIdentity;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.TraceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives spans keyed by trace_id and stores them. A {@link TraceEmitCallback} is scheduled to run
 * after the {@link RawSpansProcessor#groupingWindowTimeoutMs} interval to emit the trace. If any
 * spans for the trace arrive within the {@link RawSpansProcessor#groupingWindowTimeoutMs} interval
 * then the trace will get an additional {@link RawSpansProcessor#groupingWindowTimeoutMs} time to
 * accept spans.
 */
public class RawSpansProcessor
    implements Transformer<TraceIdentity, RawSpan, KeyValue<TraceIdentity, StructuredTrace>> {

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
  private ProcessorContext context;
  private KeyValueStore<SpanIdentity, RawSpan> spanStore;
  private KeyValueStore<TraceIdentity, TraceState> traceStateStore;
  private long groupingWindowTimeoutMs;
  private To outputTopic;
  private double dataflowSamplingPercent = -1;
  private static final Map<String, Long> maxSpanCountMap = new HashMap<>();
  private long defaultMaxSpanCountLimit = Long.MAX_VALUE;
  private CallbackRegistryPunctuator<TraceIdentity> traceEmitCallbackRegistry;
  private Cancellable traceEmitCallbackRegistryCancellable;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.spanStore =
        (KeyValueStore<SpanIdentity, RawSpan>) context.getStateStore(SPAN_STATE_STORE_NAME);
    this.traceStateStore =
        (KeyValueStore<TraceIdentity, TraceState>) context.getStateStore(TRACE_STATE_STORE);
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

    if (jobConfig.hasPath(DEFAULT_INFLIGHT_TRACE_MAX_SPAN_COUNT)) {
      defaultMaxSpanCountLimit = jobConfig.getLong(DEFAULT_INFLIGHT_TRACE_MAX_SPAN_COUNT);
    }

    this.outputTopic = To.child(OUTPUT_TOPIC_PRODUCER);

    TraceEmitCallback traceEmitCallback =
        new TraceEmitCallback(
            context,
            spanStore,
            traceStateStore,
            outputTopic,
            groupingWindowTimeoutMs,
            dataflowSamplingPercent);
    KeyValueStore<Long, List<TraceIdentity>> traceEmitCallbackRegistryStore =
        context.getStateStore(TRACE_EMIT_CALLBACK_REGISTRY_STORE_NAME);
    traceEmitCallbackRegistry =
        new CallbackRegistryPunctuator<>(
            new CallbackRegistryPunctuatorConfig(
                jobConfig.getDuration(TRACE_EMIT_CALLBACK_REGISTRY_YIELD_CONFIG_KEY).toMillis(),
                jobConfig.getDuration(TRACE_EMIT_CALLBACK_REGISTRY_WINDOW_CONFIG_KEY).toMillis()),
            traceEmitCallbackRegistryStore,
            traceEmitCallback::callback);
    traceEmitCallbackRegistryCancellable =
        context.schedule(
            jobConfig.getDuration(TRACE_EMIT_CALLBACK_REGISTRY_FREQUENCY_CONFIG_KEY),
            PunctuationType.WALL_CLOCK_TIME,
            traceEmitCallbackRegistry);
  }

  public KeyValue<TraceIdentity, StructuredTrace> transform(TraceIdentity key, RawSpan value) {
    Instant start = Instant.now();
    long currentTimeMs = System.currentTimeMillis();

    TraceState traceState = traceStateStore.get(key);
    boolean firstEntry = (traceState == null);

    if (shouldDropSpan(key, traceState)) {
      return null;
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
      traceEmitCallbackRegistry.add(currentTimeMs, key);
    } else {
      traceState.getSpanIds().add(spanId);
      traceState.setTraceEndTimestamp(currentTimeMs);
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
    int inFlightSpansPerTrace =
        traceState != null ? traceState.getSpanIds().size() : Integer.MIN_VALUE;
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
    traceEmitCallbackRegistryCancellable.cancel();
  }
}
