package org.hypertrace.core.rawspansgrouper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.hypertrace.core.datamodel.RawSpan;
import org.junit.jupiter.api.Test;

public class RawSpanToStructuredTraceGroupAggregatorTest {
  @Test
  public void testRawSpanToStructuredTraceGroupAggregatorSimpleMethods() {
    RawSpanToStructuredTraceAvroGroupAggregator aggregator = new RawSpanToStructuredTraceAvroGroupAggregator();
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
  }
}
