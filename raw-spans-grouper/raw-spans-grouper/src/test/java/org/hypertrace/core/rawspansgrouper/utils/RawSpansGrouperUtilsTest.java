package org.hypertrace.core.rawspansgrouper.utils;

import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.TRACE_CREATION_TIME;

import java.util.Map;
import org.hypertrace.core.datamodel.TimestampRecord;
import org.hypertrace.core.datamodel.Timestamps;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RawSpansGrouperUtilsTest {

  @Test
  void testTrackEndToEndLatencyTimestamps() {
    RawSpansGrouperUtils rawSpansGrouperUtils = new RawSpansGrouperUtils(100);
    Timestamps timestamps = rawSpansGrouperUtils.trackEndToEndLatencyTimestamps(123L, 123L);
    Assertions.assertNull(timestamps);

    rawSpansGrouperUtils = new RawSpansGrouperUtils(0);
    timestamps = rawSpansGrouperUtils.trackEndToEndLatencyTimestamps(150L, 100L);
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
