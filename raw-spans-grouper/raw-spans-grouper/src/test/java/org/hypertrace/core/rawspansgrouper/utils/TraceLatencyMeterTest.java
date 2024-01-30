package org.hypertrace.core.rawspansgrouper.utils;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_CREATION_TIME;

import java.util.Map;
import org.hypertrace.core.datamodel.TimestampRecord;
import org.hypertrace.core.datamodel.Timestamps;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TraceLatencyMeterTest {

  @Test
  void testTrackEndToEndLatencyTimestamps() {
    TraceLatencyMeter traceLatencyMeter = new TraceLatencyMeter(100);
    Timestamps timestamps = traceLatencyMeter.trackEndToEndLatencyTimestamps(123L, 123L);
    Assertions.assertNull(timestamps);

    traceLatencyMeter = new TraceLatencyMeter(0);
    timestamps = traceLatencyMeter.trackEndToEndLatencyTimestamps(150L, 100L);
    Assertions.assertEquals(
        new Timestamps(
            Map.of(
                DataflowMetricUtils.SPAN_ARRIVAL_TIME,
                new TimestampRecord(DataflowMetricUtils.SPAN_ARRIVAL_TIME, 100L),
                TRACE_CREATION_TIME,
                new TimestampRecord(TRACE_CREATION_TIME, 150L))),
        timestamps);
  }
}
