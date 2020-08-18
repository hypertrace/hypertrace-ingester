package org.hypertrace.core.rawspansgrouper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.junit.jupiter.api.Test;

public class RawSpanToStructuredTraceGroupAggregatorTest {

  private final String DATAFLOW_SAMPLING_PERCENT = "dataflow.metriccollection.sampling.percent";

  @Test
  public void testRawSpanToStructuredTraceGroupAggregatorSimpleMethods() {
    Config config = mock(Config.class);
    when(config.hasPath(DATAFLOW_SAMPLING_PERCENT)).thenReturn(true);
    when(config.getDouble(DATAFLOW_SAMPLING_PERCENT)).thenReturn(100.0);
    RawSpanToStructuredTraceAvroGroupAggregator aggregator =
        new RawSpanToStructuredTraceAvroGroupAggregator(config);
    List<RawSpan> rawSpanList = aggregator.createAccumulator();
    assertNotNull(rawSpanList);
    assertTrue(rawSpanList.isEmpty());

    RawSpan rawSpan1 = mock(RawSpan.class);
    RawSpan rawSpan2 = mock(RawSpan.class);
    RawSpan rawSpan3 = mock(RawSpan.class);
    RawSpan rawSpan4 = mock(RawSpan.class);

    aggregator.add(rawSpan1, rawSpanList);
    aggregator.add(rawSpan2, rawSpanList);

    assertFalse(rawSpanList.isEmpty());
    assertEquals(rawSpan1, rawSpanList.get(0));
    assertEquals(rawSpan2, rawSpanList.get(1));

    List<RawSpan> rawSpanList1 = List.of(rawSpan1, rawSpan2);
    List<RawSpan> rawSpanList2 = List.of(rawSpan3, rawSpan4);

    List<RawSpan> mergedRawSpans = aggregator.merge(rawSpanList1, rawSpanList2);

    assertEquals(4, mergedRawSpans.size());
    assertEquals(rawSpan1, mergedRawSpans.get(0));
    assertEquals(rawSpan2, mergedRawSpans.get(1));
    assertEquals(rawSpan3, mergedRawSpans.get(2));
    assertEquals(rawSpan4, mergedRawSpans.get(3));

    ByteBuffer buffer = mock(ByteBuffer.class);
    Event event = mock(Event.class);
    when(rawSpan1.getTraceId()).thenReturn(buffer);
    when(rawSpan1.getCustomerId()).thenReturn("abc123");
    when(event.getEventId()).thenReturn(buffer);
    when(rawSpan1.getEvent()).thenReturn(event);
    StructuredTrace trace = aggregator.getResult(List.of(rawSpan1));
    assertEquals("abc123", trace.getCustomerId());
  }
}
