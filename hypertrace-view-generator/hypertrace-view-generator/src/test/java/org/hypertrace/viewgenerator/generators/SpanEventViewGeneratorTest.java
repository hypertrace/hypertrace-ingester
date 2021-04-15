package org.hypertrace.viewgenerator.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.viewgenerator.api.SpanEventView;
import org.hypertrace.viewgenerator.generators.ViewGeneratorState.TraceState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SpanEventViewGeneratorTest {
  private SpanEventViewGenerator spanEventViewGenerator;

  @BeforeEach
  public void setup() {
    spanEventViewGenerator = new SpanEventViewGenerator();
  }

  @Test
  public void test_getRequestUrl_nullProtocol_shouldReturnNull() {
    Event event = mock(Event.class);
    Assertions.assertNull(spanEventViewGenerator.getRequestUrl(event, null));
  }

  @Test
  public void test_getRequestUrl_httpProtocol_shouldReturnFullUrl() {
    Event event = mock(Event.class);
    when(event.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(Request.newBuilder().setUrl("http://www.example.com").build())
                .build());
    assertEquals(
        "http://www.example.com",
        spanEventViewGenerator.getRequestUrl(event, Protocol.PROTOCOL_HTTP));
  }

  @Test
  public void test_getRequestUrl_httpsProtocol_shouldReturnFullUrl() {
    Event event = mock(Event.class);
    when(event.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(Request.newBuilder().setUrl("https://www.example.com").build())
                .build());
    assertEquals(
        "https://www.example.com",
        spanEventViewGenerator.getRequestUrl(event, Protocol.PROTOCOL_HTTPS));
  }

  @Test
  public void test_getRequestUrl_grpcProctol_shouldReturnEventName() {
    Event event = mock(Event.class);
    when(event.getEventName()).thenReturn("Sent.hipstershop.AdService.GetAds");
    assertEquals(
        "Sent.hipstershop.AdService.GetAds",
        spanEventViewGenerator.getRequestUrl(event, Protocol.PROTOCOL_GRPC));
  }

  @Test
  public void testGetRequestUrl_fullUrlIsAbsent() {
    Event event = mock(Event.class);
    when(event.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(Request.newBuilder().setPath("/api/v1/gatekeeper/check").build())
                .build());
    assertEquals(
        "/api/v1/gatekeeper/check",
        spanEventViewGenerator.getRequestUrl(event, Protocol.PROTOCOL_HTTP));
  }

  @Test
  public void testGetRequestUrl_urlAndPathIsAbsent() {
    Event event = mock(Event.class);
    when(event.getHttp())
        .thenReturn(Http.newBuilder().setRequest(Request.newBuilder().build()).build());
    Assertions.assertNull(spanEventViewGenerator.getRequestUrl(event, Protocol.PROTOCOL_HTTP));
  }

  @Test
  public void testSpanEventViewGen_HotrodTrace() throws IOException {
    URL resource =
        Thread.currentThread().getContextClassLoader().getResource("StructuredTrace-Hotrod.avro");

    SpecificDatumReader<StructuredTrace> datumReader =
        new SpecificDatumReader<>(StructuredTrace.getClassSchema());
    DataFileReader<StructuredTrace> dfrStructuredTrace =
        new DataFileReader<>(new File(resource.getPath()), datumReader);
    StructuredTrace trace = dfrStructuredTrace.next();
    dfrStructuredTrace.close();

    TraceState traceState = new TraceState(trace);
    verifyGetExitSpanToApiEntrySpan_HotrodTrace(trace, traceState);
    SpanEventViewGenerator spanEventViewGenerator = new SpanEventViewGenerator();
    List<SpanEventView> spanEventViews = spanEventViewGenerator.process(trace);
    assertEquals(50, spanEventViews.size());
  }

  private void verifyGetExitSpanToApiEntrySpan_HotrodTrace(
      StructuredTrace trace, TraceState traceState) {
    Map<ByteBuffer, Event> exitSpanToApiEntrySpanMap =
        spanEventViewGenerator.getExitSpanToCalleeApiEntrySpanMap(
            trace.getEventList(), traceState.getChildToParentEventIds(),
            traceState.getParentToChildrenEventIds(), traceState.getEventMap());

    // verify for all entries in the map, key is exit span and value is entry api boundary
    exitSpanToApiEntrySpanMap.forEach(
        (key, value) -> {
          EnrichedSpanUtils.isExitSpan(traceState.getEventMap().get(key));
          EnrichedSpanUtils.isEntryApiBoundary(value);
        });
  }

  @Test
  public void testExitCallsInfo() {
    StructuredTrace.Builder traceBuilder = StructuredTrace.newBuilder();
    traceBuilder
        .setCustomerId("customer1")
        .setTraceId(ByteBuffer.wrap("sample-trace-id".getBytes()))
        .setEntityList(
            Collections.singletonList(
                Entity.newBuilder()
                    .setCustomerId("customer1")
                    .setEntityId("sample-entity-id")
                    .setEntityName("sample-entity-name")
                    .setEntityType("SERVICE")
                    .build()))
        .setEventList(
            Collections.singletonList(
                Event.newBuilder()
                    .setCustomerId("customer1")
                    .setEventId(ByteBuffer.wrap("sample-span-id".getBytes()))
                    .setEventName("sample-span-name")
                    .setEntityIdList(Collections.singletonList("sample-entity-id"))
                    .setStartTimeMillis(System.currentTimeMillis())
                    .setEndTimeMillis(System.currentTimeMillis())
                    .setMetrics(Metrics.newBuilder().setMetricMap(new HashMap<>()).build())
                    .setAttributesBuilder(Attributes.newBuilder().setAttributeMap(new HashMap<>()))
                    .setEnrichedAttributesBuilder(
                        Attributes.newBuilder().setAttributeMap(Maps.newHashMap()))
                    .build()))
        .setMetrics(Metrics.newBuilder().setMetricMap(new HashMap<>()).build())
        .setEntityEdgeList(new ArrayList<>())
        .setEventEdgeList(new ArrayList<>())
        .setEntityEventEdgeList(new ArrayList<>())
        .setStartTimeMillis(System.currentTimeMillis())
        .setEndTimeMillis(System.currentTimeMillis());

    StructuredTrace trace = traceBuilder.build();
    SpanEventViewGenerator spanEventViewGenerator = new SpanEventViewGenerator();
    List<SpanEventView> list = spanEventViewGenerator.process(trace);
    assertEquals(Maps.newHashMap(), list.get(0).getApiCalleeNameCount());
    assertEquals(0, list.get(0).getApiExitCalls());

    Map<String, AttributeValue> spanAttributes = new HashMap<>();
    spanAttributes.put(
        EnrichedSpanConstants.API_EXIT_CALLS_ATTRIBUTE,
        AttributeValue.newBuilder().setValue("5").build());
    Map<String, String> calleeNameCount = Map.of("service1", "5", "backend1", "2");
    spanAttributes.put(
        EnrichedSpanConstants.API_CALLEE_NAME_COUNT_ATTRIBUTE,
        AttributeValue.newBuilder().setValueMap(calleeNameCount).build());
    spanAttributes.put(
        EnrichedSpanConstants.getValue(Api.API_BOUNDARY_TYPE),
        AttributeValueCreator.create(
            EnrichedSpanConstants.getValue(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY)));

    traceBuilder
        .setEventList(
            Collections.singletonList(
                Event.newBuilder()
                    .setCustomerId("customer1")
                    .setEventId(ByteBuffer.wrap("sample-span-id".getBytes()))
                    .setEventName("sample-span-name")
                    .setEntityIdList(Collections.singletonList("sample-entity-id"))
                    .setStartTimeMillis(System.currentTimeMillis())
                    .setEndTimeMillis(System.currentTimeMillis())
                    .setMetrics(Metrics.newBuilder().setMetricMap(new HashMap<>()).build())
                    .setAttributesBuilder(Attributes.newBuilder().setAttributeMap(new HashMap<>()))
                    .setEnrichedAttributesBuilder(
                        Attributes.newBuilder().setAttributeMap(spanAttributes))
                    .build()))
        .build();

    trace = traceBuilder.build();
    spanEventViewGenerator = new SpanEventViewGenerator();
    list = spanEventViewGenerator.process(trace);
    assertEquals(calleeNameCount, list.get(0).getApiCalleeNameCount());
    assertEquals(5, list.get(0).getApiExitCalls());
  }

  @Test
  public void testApiTraceErrorSpanCount() {
    StructuredTrace.Builder traceBuilder = StructuredTrace.newBuilder();
    traceBuilder
        .setCustomerId("customer1")
        .setTraceId(ByteBuffer.wrap("sample-trace-id".getBytes()))
        .setEntityList(
            Collections.singletonList(
                Entity.newBuilder()
                    .setCustomerId("customer1")
                    .setEntityId("sample-entity-id")
                    .setEntityName("sample-entity-name")
                    .setEntityType("SERVICE")
                    .build()))
        .setEventList(
            Collections.singletonList(
                Event.newBuilder()
                    .setCustomerId("customer1")
                    .setEventId(ByteBuffer.wrap("sample-span-id".getBytes()))
                    .setEventName("sample-span-name")
                    .setEntityIdList(Collections.singletonList("sample-entity-id"))
                    .setStartTimeMillis(System.currentTimeMillis())
                    .setEndTimeMillis(System.currentTimeMillis())
                    .setMetrics(Metrics.newBuilder().setMetricMap(new HashMap<>()).build())
                    .setAttributesBuilder(Attributes.newBuilder().setAttributeMap(new HashMap<>()))
                    .setEnrichedAttributesBuilder(
                        Attributes.newBuilder().setAttributeMap(Maps.newHashMap()))
                    .build()))
        .setMetrics(Metrics.newBuilder().setMetricMap(new HashMap<>()).build())
        .setEntityEdgeList(new ArrayList<>())
        .setEventEdgeList(new ArrayList<>())
        .setEntityEventEdgeList(new ArrayList<>())
        .setStartTimeMillis(System.currentTimeMillis())
        .setEndTimeMillis(System.currentTimeMillis());

    StructuredTrace trace = traceBuilder.build();
    SpanEventViewGenerator spanEventViewGenerator = new SpanEventViewGenerator();
    List<SpanEventView> list = spanEventViewGenerator.process(trace);
    assertEquals(0, list.get(0).getApiTraceErrorSpanCount());

    Map<String, AttributeValue> spanAttributes = new HashMap<>();
    spanAttributes.put(
        EnrichedSpanConstants.API_TRACE_ERROR_SPAN_COUNT_ATTRIBUTE,
        AttributeValue.newBuilder().setValue("5").build());

    traceBuilder
        .setEventList(
            Collections.singletonList(
                Event.newBuilder()
                    .setCustomerId("customer1")
                    .setEventId(ByteBuffer.wrap("sample-span-id".getBytes()))
                    .setEventName("sample-span-name")
                    .setEntityIdList(Collections.singletonList("sample-entity-id"))
                    .setStartTimeMillis(System.currentTimeMillis())
                    .setEndTimeMillis(System.currentTimeMillis())
                    .setMetrics(Metrics.newBuilder().setMetricMap(new HashMap<>()).build())
                    .setAttributesBuilder(Attributes.newBuilder().setAttributeMap(new HashMap<>()))
                    .setEnrichedAttributesBuilder(
                        Attributes.newBuilder().setAttributeMap(spanAttributes))
                    .build()))
        .build();

    trace = traceBuilder.build();
    spanEventViewGenerator = new SpanEventViewGenerator();
    list = spanEventViewGenerator.process(trace);
    assertEquals(5, list.get(0).getApiTraceErrorSpanCount());
  }
}
