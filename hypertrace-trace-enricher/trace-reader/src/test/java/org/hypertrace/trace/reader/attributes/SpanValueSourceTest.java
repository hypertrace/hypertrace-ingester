package org.hypertrace.trace.reader.attributes;

import static org.hypertrace.trace.reader.attributes.AvroUtil.buildAttributesWithKeyValue;
import static org.hypertrace.trace.reader.attributes.AvroUtil.buildMetricsWithKeyValue;
import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedEventBuilder;
import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedStructuredTraceBuilder;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.doubleLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.longLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.stringLiteral;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.Resource;
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
        new SpanValueSource(mock(StructuredTrace.class), span)
            .getAttribute("spanKey", AttributeKind.TYPE_STRING));

    assertEquals(
        Optional.empty(),
        new SpanValueSource(mock(StructuredTrace.class), span)
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
        new SpanValueSource(mock(StructuredTrace.class), span)
            .getAttribute("spanKey", AttributeKind.TYPE_STRING));
  }

  @Test
  void canReadMetrics() {
    Event span =
        defaultedEventBuilder().setMetrics(buildMetricsWithKeyValue("spanKey", 10.3)).build();

    assertEquals(
        Optional.of(doubleLiteral(10.3)),
        new SpanValueSource(mock(StructuredTrace.class), span)
            .getMetric("spanKey", AttributeKind.TYPE_DOUBLE));

    assertEquals(
        Optional.of(longLiteral(10)),
        new SpanValueSource(mock(StructuredTrace.class), span)
            .getMetric("spanKey", AttributeKind.TYPE_INT64));

    assertEquals(
        Optional.empty(),
        new SpanValueSource(mock(StructuredTrace.class), span)
            .getMetric("fake", AttributeKind.TYPE_INT64));
  }

  @Test
  void canConvertValueSourceBasedOnScope() {
    StructuredTrace mockTrace = mock(StructuredTrace.class);
    SpanValueSource originalSource = new SpanValueSource(mockTrace, mock(Event.class));
    assertEquals(Optional.of(originalSource), originalSource.sourceForScope("OTHER"));
    assertEquals(
        Optional.of(ValueSourceFactory.forTrace(mockTrace)),
        originalSource.sourceForScope("TRACE"));
  }

  @Test
  void returnsEmptyOptionalIfNoResource() {
    Event span = defaultedEventBuilder().build();
    assertEquals(
        Optional.empty(),
        new SpanValueSource(mock(StructuredTrace.class), span)
            .getAttribute("resourceKey", AttributeKind.TYPE_STRING));
  }

  @Test
  void returnsEmptyOptionalIfNoMatchingKeyInResource() {
    StructuredTrace trace =
        defaultedStructuredTraceBuilder()
            .setResourceList(
                List.of(
                    Resource.newBuilder()
                        .setAttributes(buildAttributesWithKeyValue("otherResourceKey", "value"))
                        .build()))
            .build();

    Event span = defaultedEventBuilder().setResourceIndex(0).build();

    assertEquals(
        Optional.empty(),
        new SpanValueSource(mock(StructuredTrace.class), span)
            .getAttribute("resourceKey", AttributeKind.TYPE_STRING));
  }

  @Test
  void returnsValueIfMatchingKeyInResource() {
    StructuredTrace trace =
        defaultedStructuredTraceBuilder()
            .setResourceList(
                List.of(
                    mock(Resource.class), // idx 0 won't be used in this test
                    Resource.newBuilder()
                        .setAttributes(buildAttributesWithKeyValue("resourceKey", "value"))
                        .build()))
            .build();

    Event span = defaultedEventBuilder().setResourceIndex(1).build();

    assertEquals(
        Optional.of(stringLiteral("value")),
        new SpanValueSource(trace, span).getAttribute("resourceKey", AttributeKind.TYPE_STRING));
  }
}
