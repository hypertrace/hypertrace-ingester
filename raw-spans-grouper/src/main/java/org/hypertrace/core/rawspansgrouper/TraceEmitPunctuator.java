package org.hypertrace.core.rawspansgrouper;

import io.micrometer.core.instrument.Timer;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.IndexedRecord;
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
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks if a trace can be finalized and emitted based on inactivity period of {@link
 * RawSpansGroupingTransformer#groupingWindowTimeoutMs}
 */
public class TraceEmitPunctuator implements Punctuator {

  private static final Logger logger = LoggerFactory.getLogger(TraceEmitPunctuator.class);

  private double dataflowSamplingPercent = -1;
  static final String TRACE_CREATION_TIME = "trace.creation.time";
  private static final Timer spansGrouperArrivalLagTimer =
      PlatformMetricsRegistry.registerTimer(DataflowMetricUtils.ARRIVAL_LAG, new HashMap<>());

  private String key;
  private ProcessorContext context;
  private KeyValueStore<String, ValueAndTimestamp<RawSpans>> inflightTraceStore;
  private KeyValueStore<String, Long> traceEmitTriggerStore;
  private To outputTopicProducer;
  private long groupingWindowTimeoutMs;
  private Cancellable cancellable;

  public TraceEmitPunctuator(String key, ProcessorContext context,
      KeyValueStore<String, ValueAndTimestamp<RawSpans>> inflightTraceStore,
      KeyValueStore<String, Long> traceEmitTriggerStore,
      To outputTopicProducer,
      long groupingWindowTimeoutMs) {
    this.key = key;
    this.context = context;
    this.inflightTraceStore = inflightTraceStore;
    this.traceEmitTriggerStore = traceEmitTriggerStore;
    this.outputTopicProducer = outputTopicProducer;
    this.groupingWindowTimeoutMs = groupingWindowTimeoutMs;
  }

  public void setCancellable(Cancellable cancellable) {
    this.cancellable = cancellable;
  }

  /**
   * @param timestamp correspond to current system time
   */
  @Override
  public void punctuate(long timestamp) {
    // always cancel the punctuator else it will get re-scheduled automatically
    cancellable.cancel();

    Long emitTs = traceEmitTriggerStore.delete(key);
    if (emitTs <= timestamp) {
      // Implies that no new spans for the trace have arrived within the last 'groupingWindowTimeoutMs' interval
      // so the trace can be finalized and emitted
      ValueAndTimestamp<RawSpans> agg = inflightTraceStore.delete(key);
      ByteBuffer traceId = null;
      String customerId = null;
      if (!agg.value().getRawSpans().isEmpty()) {
        RawSpan firstSpan = agg.value().getRawSpans().get(0);
        traceId = firstSpan.getTraceId();
        customerId = firstSpan.getCustomerId();
      }
      List<RawSpan> rawSpanList = agg.value().getRawSpans();
      Timestamps timestamps = trackEndToEndLatencyTimestamps(timestamp, agg);
      StructuredTrace trace = StructuredTraceBuilder
          .buildStructuredTraceFromRawSpans(rawSpanList, traceId, customerId,
              timestamps);

      logger.debug("Emit trace_id=[{}], spans_count=[{}]", key, rawSpanList.size());
      context.forward(null, trace, outputTopicProducer);
    } else {
      // implies spans for the trace have arrived within the last 'sessionTimeoutMs' interval
      // so the session inactivity window is extended from the last timestamp
      if (logger.isDebugEnabled()) {
        logger.debug("Re-scheduling emit trigger for trace_id=[{}] to [{}]", key,
            Instant.ofEpochMilli(emitTs + groupingWindowTimeoutMs));
      }
      long newEmitTs = emitTs + groupingWindowTimeoutMs;
      traceEmitTriggerStore.put(key, newEmitTs);
      // if current timestamp is ahead of newEmitTs then just add a grace of 100ms and fire it
      long duration = Math.max(100, newEmitTs - timestamp);
      cancellable = context
          .schedule(Duration.ofMillis(duration), PunctuationType.WALL_CLOCK_TIME, this);
    }
  }

  private Timestamps trackEndToEndLatencyTimestamps(long timestamp,
      ValueAndTimestamp<RawSpans> agg) {
    Timestamps timestamps = null;
    if (!agg.value().getRawSpans().isEmpty() && Math.random() * 100 <= dataflowSamplingPercent) {
      long currentTime = timestamp;
      long firstSpanArrivalTime = agg.value().getRawSpans().get(0).getReceivedTimeMillis();
      spansGrouperArrivalLagTimer
          .record(currentTime - firstSpanArrivalTime, TimeUnit.MILLISECONDS);
      Map<String, TimestampRecord> records = new HashMap<>();
      records.put(DataflowMetricUtils.SPAN_ARRIVAL_TIME,
          new TimestampRecord(DataflowMetricUtils.SPAN_ARRIVAL_TIME,
              firstSpanArrivalTime));
      records
          .put(TRACE_CREATION_TIME,
              new TimestampRecord(TRACE_CREATION_TIME, currentTime));
      timestamps = new Timestamps(records);
    }
    return timestamps;
  }
}
