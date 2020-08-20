package org.hypertrace.core.rawspansgrouper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import io.micrometer.core.instrument.Timer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.TimestampRecord;
import org.hypertrace.core.datamodel.Timestamps;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

public class RawSpanToStructuredTraceAvroGroupAggregator implements
    AggregateFunction<RawSpan, List<RawSpan>, StructuredTrace> {

  private double dataflowSamplingPercent = -1;
  private final String DATAFLOW_SAMPLING_PERCENT = "dataflow.metriccollection.sampling.percent";
  private static final String TRACE_CREATION_TIME = "trace.creation.time";
  private static final Timer spansGrouperArrivalLagTimer =
      PlatformMetricsRegistry.registerTimer(DataflowMetricUtils.ARRIVAL_LAG, new HashMap<>());

  public RawSpanToStructuredTraceAvroGroupAggregator(Config config) {
    if (config.hasPath(DATAFLOW_SAMPLING_PERCENT)
        && config.getDouble(DATAFLOW_SAMPLING_PERCENT) > 0 && config.getDouble(DATAFLOW_SAMPLING_PERCENT) <= 100) {
      this.dataflowSamplingPercent = config.getDouble(DATAFLOW_SAMPLING_PERCENT);
    }
  }

  @Override
  public List<RawSpan> createAccumulator() {
    return new ArrayList<>();
  }

  @Override
  public List<RawSpan> add(RawSpan value, List<RawSpan> accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public StructuredTrace getResult(List<RawSpan> accumulator) {
    // These raw spans are by Customer ID and Trace ID.
    // So the raw spans will belong to the same customer and trace
    ByteBuffer traceId = null;
    String customerId = null;
    if (!accumulator.isEmpty()) {
      RawSpan firstSpan = accumulator.get(0);
      traceId = firstSpan.getTraceId();
      customerId = firstSpan.getCustomerId();
    }
    List<RawSpan> rawSpanList = new ArrayList<>();
    for (IndexedRecord r : accumulator) {
      rawSpanList.add((RawSpan) r);
    }

    Timestamps timestamps = null;
    if (!accumulator.isEmpty() && Math.random()*100 <= dataflowSamplingPercent) {
      long currentTime = System.currentTimeMillis();
      long firstSpanArrivalTime = accumulator.get(0).getReceivedTimeMillis();
      spansGrouperArrivalLagTimer.record(currentTime - firstSpanArrivalTime, TimeUnit.MILLISECONDS);
      Map<String, TimestampRecord> records = new HashMap<>();
      records.put(DataflowMetricUtils.SPAN_ARRIVAL_TIME,
          new TimestampRecord(DataflowMetricUtils.SPAN_ARRIVAL_TIME, firstSpanArrivalTime));
      records.put(TRACE_CREATION_TIME, new TimestampRecord(TRACE_CREATION_TIME, currentTime));
      timestamps = new Timestamps(records);
    }

    return StructuredTraceBuilder
        .buildStructuredTraceFromRawSpans(rawSpanList, traceId, customerId, timestamps);
  }

  @Override
  public List<RawSpan> merge(List<RawSpan> a, List<RawSpan> b) {
    List<RawSpan> res = createAccumulator();
    res.addAll(a);
    res.addAll(b);
    return res;
  }
}
