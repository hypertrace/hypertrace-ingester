package org.hypertrace.trace.reader;

import static org.hypertrace.trace.reader.AvroUtil.buildAttributesWithKeyValue;
import static org.hypertrace.trace.reader.AvroUtil.buildMetricsWithKeyValue;
import static org.hypertrace.trace.reader.AvroUtil.defaultedEventBuilder;
import static org.hypertrace.trace.reader.LiteralValueUtil.doubleLiteral;
import static org.hypertrace.trace.reader.LiteralValueUtil.longLiteral;
import static org.hypertrace.trace.reader.LiteralValueUtil.stringLiteral;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.junit.jupiter.api.Test;

class SpanValueSourceTest {

  @Test
  void canReadAttributes() {
    Event span =
        defaultedEventBuilder()
            .setAttributes(buildAttributesWithKeyValue("spanKey", "spanValue"))
            .build();

    assertEquals(
        Optional.of(stringLiteral("spanValue")),
        new SpanValueSource(mock(StructuredTrace.class), span, DefaultValueCoercer.INSTANCE)
            .getAttribute("spanKey", AttributeKind.TYPE_STRING));

    assertEquals(
        Optional.empty(),
        new SpanValueSource(mock(StructuredTrace.class), span, DefaultValueCoercer.INSTANCE)
            .getAttribute("fake", AttributeKind.TYPE_STRING));
  }

  @Test
  void prefersEnrichedAttributesOverRaw() {
    Event span =
        defaultedEventBuilder()
            .setAttributes(buildAttributesWithKeyValue("spanKey", "rawValue"))
            .setEnrichedAttributes(buildAttributesWithKeyValue("spanKey", "enrichedValue"))
            .build();

    assertEquals(
        Optional.of(stringLiteral("enrichedValue")),
        new SpanValueSource(mock(StructuredTrace.class), span, DefaultValueCoercer.INSTANCE)
            .getAttribute("spanKey", AttributeKind.TYPE_STRING));
  }

  @Test
  void canReadMetrics() {
    Event span =
        defaultedEventBuilder().setMetrics(buildMetricsWithKeyValue("spanKey", 10.3)).build();

    assertEquals(
        Optional.of(doubleLiteral(10.3)),
        new SpanValueSource(mock(StructuredTrace.class), span, DefaultValueCoercer.INSTANCE)
            .getMetric("spanKey", AttributeKind.TYPE_DOUBLE));

    assertEquals(
        Optional.of(longLiteral(10)),
        new SpanValueSource(mock(StructuredTrace.class), span, DefaultValueCoercer.INSTANCE)
            .getMetric("spanKey", AttributeKind.TYPE_INT64));

    assertEquals(
        Optional.empty(),
        new SpanValueSource(mock(StructuredTrace.class), span, DefaultValueCoercer.INSTANCE)
            .getMetric("fake", AttributeKind.TYPE_INT64));
  }

  @Test
  void canConvertValueSourceBasedOnScope() {
    StructuredTrace mockTrace = mock(StructuredTrace.class);
    SpanValueSource originalSource =
        new SpanValueSource(mockTrace, mock(Event.class), DefaultValueCoercer.INSTANCE);
    assertEquals(Optional.of(originalSource), originalSource.sourceForScope("OTHER"));
    assertEquals(
        Optional.of(ValueSource.forTrace(mockTrace)), originalSource.sourceForScope("TRACE"));
  }
}
