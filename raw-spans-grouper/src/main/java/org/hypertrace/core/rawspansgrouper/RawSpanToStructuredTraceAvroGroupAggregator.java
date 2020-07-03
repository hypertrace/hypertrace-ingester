package org.hypertrace.core.rawspansgrouper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;

public class RawSpanToStructuredTraceAvroGroupAggregator implements
    AggregateFunction<RawSpan, List<RawSpan>, StructuredTrace> {

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

    return StructuredTraceBuilder
        .buildStructuredTraceFromRawSpans(rawSpanList, traceId, customerId);
  }

  @Override
  public List<RawSpan> merge(List<RawSpan> a, List<RawSpan> b) {
    List<RawSpan> res = createAccumulator();
    res.addAll(a);
    res.addAll(b);
    return res;
  }
}
