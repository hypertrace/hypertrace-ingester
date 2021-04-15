package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.semantic.convention.constants.error.OTelErrorSemanticConventions;
import org.hypertrace.core.span.constants.v1.Error;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ApiStatus;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ErrorMetrics;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ErrorsAndExceptionsEnricherTest extends AbstractAttributeEnricherTest {
  private static final Api API_STATUS = Api.API_STATUS;

  @Test
  public void noAttributes() throws IOException {
    ErrorsAndExceptionsEnricher enricher = new ErrorsAndExceptionsEnricher();
    Event e = mock(Event.class);
    when(e.getAttributes()).thenReturn(null);
    enricher.enrichEvent(null, e);
    e = createMockEvent();
    enricher.enrichEvent(null, e);

    StructuredTrace trace = createStructuredTrace("test-id", getEvent());
    enricher.enrichTrace(trace);
  }

  @Test
  public void test_failStatus_shouldGetError() {
    ErrorsAndExceptionsEnricher enricher = new ErrorsAndExceptionsEnricher();
    Event e1 = createMockEvent();
    Map<String, AttributeValue> attributeValueMap = e1.getAttributes().getAttributeMap();
    attributeValueMap.put(
        Constants.getEnrichedSpanConstant(API_STATUS),
        AttributeValue.newBuilder()
            .setValue(Constants.getEnrichedSpanConstant(ApiStatus.API_STATUS_FAIL))
            .build());
    enricher.enrichEvent(null, e1);
    Assertions.assertEquals(
        1.0d,
        e1.getMetrics()
            .getMetricMap()
            .get(Constants.getEnrichedSpanConstant(ErrorMetrics.ERROR_METRICS_ERROR_COUNT))
            .getValue());
  }

  @Test
  public void test_successStatus_shouldNotGetError() {
    ErrorsAndExceptionsEnricher enricher = new ErrorsAndExceptionsEnricher();
    // Negative test
    Event e2 = createMockEvent();
    Map<String, AttributeValue> attributeValueMap = e2.getAttributes().getAttributeMap();
    attributeValueMap.put(
        Constants.getEnrichedSpanConstant(API_STATUS),
        AttributeValue.newBuilder()
            .setValue(Constants.getEnrichedSpanConstant(ApiStatus.API_STATUS_SUCCESS))
            .build());
    enricher.enrichEvent(null, e2);
    Assertions.assertNull(
        e2.getMetrics()
            .getMetricMap()
            .get(Constants.getEnrichedSpanConstant(ErrorMetrics.ERROR_METRICS_ERROR_COUNT)));
  }

  @Test
  public void errorExists() throws IOException {
    ErrorsAndExceptionsEnricher enricher = new ErrorsAndExceptionsEnricher();
    Event e1 = getEvent();
    Map<String, AttributeValue> attributeValueMap = e1.getAttributes().getAttributeMap();
    attributeValueMap.put(
        Constants.getRawSpanConstant(Error.ERROR_ERROR), AttributeValueCreator.create(true));
    enricher.enrichEvent(null, e1);
    Assertions.assertEquals(
        1.0d,
        e1.getMetrics()
            .getMetricMap()
            .get(Constants.getEnrichedSpanConstant(ErrorMetrics.ERROR_METRICS_ERROR_COUNT))
            .getValue());

    Event e2 = getEvent();
    attributeValueMap = e2.getAttributes().getAttributeMap();
    attributeValueMap.put(
        Constants.getRawSpanConstant(Error.ERROR_STACK_TRACE),
        AttributeValueCreator.create("test stack"));
    enricher.enrichEvent(null, e2);
    Assertions.assertEquals(
        1.0d,
        e2.getMetrics()
            .getMetricMap()
            .get(Constants.getEnrichedSpanConstant(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT))
            .getValue());

    Event e3 = getEvent();
    attributeValueMap = e3.getAttributes().getAttributeMap();
    attributeValueMap.put(
        Constants.getRawSpanConstant(Error.ERROR_ERROR).toLowerCase(),
        AttributeValueCreator.create(true));
    enricher.enrichEvent(null, e3);
    Assertions.assertEquals(
        1.0d,
        e3.getMetrics()
            .getMetricMap()
            .get(Constants.getEnrichedSpanConstant(ErrorMetrics.ERROR_METRICS_ERROR_COUNT))
            .getValue());

    Event e4 = getEvent();
    attributeValueMap = e4.getAttributes().getAttributeMap();
    attributeValueMap.put(
        Constants.getRawSpanConstant(Error.ERROR_ERROR).toLowerCase(),
        AttributeValueCreator.create(true));
    enricher.enrichEvent(null, e4);
    Assertions.assertEquals(
        1.0d,
        e4.getMetrics()
            .getMetricMap()
            .get(Constants.getEnrichedSpanConstant(ErrorMetrics.ERROR_METRICS_ERROR_COUNT))
            .getValue());

    Event e5 = getEvent();
    attributeValueMap = e5.getAttributes().getAttributeMap();
    attributeValueMap.put(
        OTelErrorSemanticConventions.EXCEPTION_TYPE.getValue().toLowerCase(),
        AttributeValueCreator.create(true));
    enricher.enrichEvent(null, e5);
    Assertions.assertEquals(
        1.0d,
        e5.getMetrics()
            .getMetricMap()
            .get(Constants.getEnrichedSpanConstant(ErrorMetrics.ERROR_METRICS_ERROR_COUNT))
            .getValue());

    Event e6 = getEvent();
    attributeValueMap = e6.getAttributes().getAttributeMap();
    attributeValueMap.put(
        OTelErrorSemanticConventions.EXCEPTION_STACKTRACE.getValue().toLowerCase(),
        AttributeValueCreator.create(true));
    enricher.enrichEvent(null, e6);
    Assertions.assertEquals(
        1.0d,
        e6.getMetrics()
            .getMetricMap()
            .get(Constants.getEnrichedSpanConstant(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT))
            .getValue());

    StructuredTrace trace = createStructuredTrace("test-id", e1, e2, e3, e4, e5, e6);

    enricher.enrichEvent(trace, e1);
    enricher.enrichEvent(trace, e2);
    enricher.enrichEvent(trace, e3);
    enricher.enrichEvent(trace, e4);
    enricher.enrichEvent(trace, e5);
    enricher.enrichEvent(trace, e6);
    trace.setMetrics(Metrics.newBuilder().setMetricMap(new HashMap<>()).build());
    Assertions.assertEquals(
        1.0d,
        e4.getMetrics()
            .getMetricMap()
            .get(Constants.getEnrichedSpanConstant(ErrorMetrics.ERROR_METRICS_ERROR_COUNT))
            .getValue());
    enricher.enrichTrace(trace);
    Assertions.assertEquals(
        6.0d,
        trace
            .getMetrics()
            .getMetricMap()
            .get(
                Constants.getEnrichedSpanConstant(
                    ErrorMetrics.ERROR_METRICS_TOTAL_SPANS_WITH_ERRORS))
            .getValue());
    Assertions.assertEquals(
        3.0d,
        trace
            .getMetrics()
            .getMetricMap()
            .get(
                Constants.getEnrichedSpanConstant(
                    ErrorMetrics.ERROR_METRICS_TOTAL_SPANS_WITH_EXCEPTIONS))
            .getValue());

    // Trace itself doesn't have an error since there was no entry span.
    Assertions.assertTrue(
        trace
            .getAttributes()
            .getAttributeMap()
            .containsKey(
                Constants.getEnrichedSpanConstant(
                    CommonAttribute.COMMON_ATTRIBUTE_TRANSACTION_HAS_ERROR)));

    // Make e1 as ENTRY span so that the error is reflected on the structured trace.
    e1.getEnrichedAttributes()
        .getAttributeMap()
        .put(
            Constants.getEnrichedSpanConstant(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE),
            AttributeValueCreator.create(
                Constants.getEnrichedSpanConstant(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY)));
    enricher.enrichTrace(trace);
    Assertions.assertEquals(
        6.0d,
        trace
            .getMetrics()
            .getMetricMap()
            .get(
                Constants.getEnrichedSpanConstant(
                    ErrorMetrics.ERROR_METRICS_TOTAL_SPANS_WITH_ERRORS))
            .getValue());
    Assertions.assertEquals(
        3.0d,
        trace
            .getMetrics()
            .getMetricMap()
            .get(
                Constants.getEnrichedSpanConstant(
                    ErrorMetrics.ERROR_METRICS_TOTAL_SPANS_WITH_EXCEPTIONS))
            .getValue());
    Assertions.assertEquals(
        "true",
        trace
            .getAttributes()
            .getAttributeMap()
            .get(
                Constants.getEnrichedSpanConstant(
                    CommonAttribute.COMMON_ATTRIBUTE_TRANSACTION_HAS_ERROR))
            .getValue());
  }

  private Event getEvent() {
    Map<String, AttributeValue> map = new HashMap<>();
    Map<String, MetricValue> metricMap = new HashMap<>();
    map.put("span.kind", AttributeValue.newBuilder().setValue("server").build());
    map.put("error", AttributeValue.newBuilder().setValue("true").build());
    map.put("SPAN_TYPE", AttributeValue.newBuilder().setValue("ENTRY").build());
    metricMap.put("Duration", MetricValue.newBuilder().setValue(4.0).build());
    Event event =
        Event.newBuilder()
            .setCustomerId("customer1")
            .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f9".getBytes()))
            .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824"))
            .setEnrichedAttributes(Attributes.newBuilder().setAttributeMap(map).build())
            .setAttributes(Attributes.newBuilder().setAttributeMap(map).build())
            .setEventName("test-event")
            .setStartTimeMillis(1566869077746L)
            .setEndTimeMillis(1566869077750L)
            .setMetrics(Metrics.newBuilder().setMetricMap(metricMap).build())
            .setEventRefList(Collections.emptyList())
            .build();

    return event;
  }
}
