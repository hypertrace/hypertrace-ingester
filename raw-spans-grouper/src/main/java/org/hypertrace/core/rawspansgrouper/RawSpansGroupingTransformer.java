package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.INFLIGHT_TRACE_STORE;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.OUTPUT_TOPIC_PRODUCER;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_EMIT_TRIGGER_STORE;

import com.typesafe.config.Config;
import java.time.Duration;
import java.time.Instant;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives spans keyed by trace_id and stores them. A {@link TraceEmitPunctuator} is scheduled to
 * run after the {@link RawSpansGroupingTransformer#groupingWindowTimeoutMs} interval to emit the trace. If
 * any spans for the trace arrive within the {@link RawSpansGroupingTransformer#groupingWindowTimeoutMs}
 * interval then the {@link RawSpansGroupingTransformer#groupingWindowTimeoutMs} will get reset and the
 * trace will get an additional {@link RawSpansGroupingTransformer#groupingWindowTimeoutMs} time to accept
 * spans.
 */
public class RawSpansGroupingTransformer implements
    Transformer<String, RawSpan, KeyValue<String, StructuredTrace>> {

  private static final Logger logger = LoggerFactory.getLogger(RawSpansGroupingTransformer.class);
  private ProcessorContext context;
  private KeyValueStore<String, ValueAndTimestamp<RawSpans>> inflightTraceStore;
  private KeyValueStore<String, Long> traceEmitTriggerStore;
  private long groupingWindowTimeoutMs;
  private To outputTopic;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.inflightTraceStore = (KeyValueStore<String, ValueAndTimestamp<RawSpans>>) context
        .getStateStore(INFLIGHT_TRACE_STORE);
    this.traceEmitTriggerStore = (KeyValueStore<String, Long>) context
        .getStateStore(TRACE_EMIT_TRIGGER_STORE);
    this.groupingWindowTimeoutMs = ((Config) (context.appConfigs().get(RAW_SPANS_GROUPER_JOB_CONFIG)))
        .getLong(SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY) * 1000;
    this.outputTopic = To.child(OUTPUT_TOPIC_PRODUCER);
    //restorePunctuators();
  }

  @Override
  public KeyValue<String, StructuredTrace> transform(String key, RawSpan value) {
    ValueAndTimestamp<RawSpans> rawSpans = inflightTraceStore.get(key);
    RawSpans agg = rawSpans != null ? rawSpans.value() : RawSpans.newBuilder().build();
    // add the new span
    agg.getRawSpans().add(value);

    long currentTimeMs = System.currentTimeMillis();

    if (logger.isDebugEnabled()) {
      logger.debug("Updating ts=[{}] for trace_id=[{}]", Instant.ofEpochMilli(currentTimeMs), key);
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
      logger.debug("Updating trigger_ts=[{}] for trace_id=[{}]", Instant.ofEpochMilli(traceEmitTs),
          key);
    }
    traceEmitTriggerStore.put(key, traceEmitTs);

    // if no prior emitTs is found then add it.
    if (firstEntry) {
      schedulePunctuator(key);
    }
    // the punctuator will emit the trace
    return null;
  }

  private void schedulePunctuator(String key) {
    TraceEmitPunctuator punctuator = new TraceEmitPunctuator(key, context, inflightTraceStore,
        traceEmitTriggerStore,
        outputTopic, groupingWindowTimeoutMs);
    Cancellable cancellable = context
        .schedule(Duration.ofMillis(groupingWindowTimeoutMs), PunctuationType.WALL_CLOCK_TIME,
            punctuator);
    punctuator.setCancellable(cancellable);
    logger.debug("Scheduled a punctuator to emit trace for trace_id=[{}] to run after [{}] ms", key,
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
    try (KeyValueIterator<String, Long> it = traceEmitTriggerStore.all()) {
      while (it.hasNext()) {
        schedulePunctuator(it.next().key);
        count++;
      }
      logger.info("Restored=[{}] punctuators, Duration=[{}]", count,
          Duration.between(start, Instant.now()));
    }
  }
}
