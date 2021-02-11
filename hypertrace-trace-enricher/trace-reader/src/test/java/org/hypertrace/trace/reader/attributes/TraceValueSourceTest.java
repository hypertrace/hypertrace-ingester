package org.hypertrace.trace.reader.attributes;

import static org.hypertrace.trace.reader.attributes.AvroUtil.buildAttributesWithKeyValue;
import static org.hypertrace.trace.reader.attributes.AvroUtil.buildMetricsWithKeyValue;
import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedStructuredTraceBuilder;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.doubleLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.longLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.stringLiteral;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.junit.jupiter.api.Test;

class TraceValueSourceTest {

  @Test
  void canReadAttributes() {
    StructuredTrace trace =
        defaultedStructuredTraceBuilder()
            .setAttributes(buildAttributesWithKeyValue("traceKey", "traceValue"))
            .build();

    assertEquals(
        Optional.of(stringLiteral("traceValue")),
        new TraceValueSource(trace).getAttribute("traceKey", AttributeKind.TYPE_STRING));

    assertEquals(
        Optional.empty(),
        new TraceValueSource(trace).getAttribute("fake", AttributeKind.TYPE_STRING));
  }

  @Test
  void canReadMetrics() {
    StructuredTrace trace =
        defaultedStructuredTraceBuilder()
            .setMetrics(buildMetricsWithKeyValue("traceKey", 10.3))
            .build();

    assertEquals(
        Optional.of(doubleLiteral(10.3)),
        new TraceValueSource(trace).getMetric("traceKey", AttributeKind.TYPE_DOUBLE));

    assertEquals(
        Optional.of(longLiteral(10)),
        new TraceValueSource(trace).getMetric("traceKey", AttributeKind.TYPE_INT64));

    assertEquals(
        Optional.empty(), new TraceValueSource(trace).getMetric("fake", AttributeKind.TYPE_INT64));
  }

  @Test
  void canConvertValueSourceBasedOnScope() {
    TraceValueSource originalSource = new TraceValueSource(mock(StructuredTrace.class));
    assertEquals(Optional.of(originalSource), originalSource.sourceForScope("TRACE"));
    assertEquals(Optional.empty(), originalSource.sourceForScope("SPAN"));
  }
}
