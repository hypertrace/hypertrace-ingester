package org.hypertrace.viewgenerator.generators;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.viewgenerator.api.SpanEventView;
import org.hypertrace.viewgenerator.generators.ViewGeneratorState.TraceState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    when(event.getHttp()).thenReturn(Http.newBuilder()
        .setRequest(Request.newBuilder()
            .setUrl("http://www.example.com")
            .build()
        ).build()
    );
    Assertions.assertEquals(
        "http://www.example.com",
        spanEventViewGenerator.getRequestUrl(event, Protocol.PROTOCOL_HTTP)
    );
  }

  @Test
  public void test_getRequestUrl_httpsProtocol_shouldReturnFullUrl() {
    Event event = mock(Event.class);
    when(event.getHttp()).thenReturn(Http.newBuilder()
        .setRequest(Request.newBuilder()
            .setUrl("https://www.example.com")
            .build()
        ).build()
    );
    Assertions.assertEquals(
        "https://www.example.com",
        spanEventViewGenerator.getRequestUrl(event, Protocol.PROTOCOL_HTTPS)
    );
  }

  @Test
  public void test_getRequestUrl_grpcProctol_shouldReturnEventName() {
    Event event = mock(Event.class);
    when(event.getEventName()).thenReturn("Sent.hipstershop.AdService.GetAds");
    Assertions.assertEquals(
        "Sent.hipstershop.AdService.GetAds",
        spanEventViewGenerator.getRequestUrl(event, Protocol.PROTOCOL_GRPC)
    );
  }

  @Test
  public void testGetRequestUrl_fullUrlIsAbsent() {
    Event event = mock(Event.class);
    when(event.getHttp()).thenReturn(Http.newBuilder()
        .setRequest(Request.newBuilder()
            .setPath("/api/v1/gatekeeper/check")
            .build()
        ).build()
    );
    Assertions.assertEquals(
        "/api/v1/gatekeeper/check",
        spanEventViewGenerator.getRequestUrl(event, Protocol.PROTOCOL_HTTP));
  }

  @Test
  public void testGetRequestUrl_urlAndPathIsAbsent() {
    Event event = mock(Event.class);
    when(event.getHttp()).thenReturn(Http.newBuilder()
        .setRequest(Request.newBuilder()
            .build()
        ).build()
    );
    Assertions.assertNull(spanEventViewGenerator.getRequestUrl(event, Protocol.PROTOCOL_HTTP));
  }

  @Test
  public void testSpanEventViewGenWithTrace() throws IOException {
    URL resource = Thread.currentThread().getContextClassLoader().
        getResource("StructuredTrace-Hotrod.avro");

    SpecificDatumReader<StructuredTrace> datumReader = new SpecificDatumReader<>(
        StructuredTrace.getClassSchema());
    DataFileReader<StructuredTrace> dfrStructuredTrace = new DataFileReader<>(new File(resource.getPath()), datumReader);
    StructuredTrace trace = dfrStructuredTrace.next();
    dfrStructuredTrace.close();

    TraceState traceState = new TraceState(trace);
    verifyCreateExitSpanToApiEntrySpan(trace, traceState);
    SpanEventViewGenerator spanEventViewGenerator = new SpanEventViewGenerator();
    List<SpanEventView> spanEventViews = spanEventViewGenerator.process(trace);
    Assertions.assertEquals(50, spanEventViews.size());
  }

  private void verifyCreateExitSpanToApiEntrySpan(
      StructuredTrace trace, TraceState traceState) {
    Map<ByteBuffer, Event> exitSpanToApiEntrySpanMap =
        spanEventViewGenerator.getExitSpanToCalleeApiEntrySpanMap(
            trace.getEventList(), traceState.getChildToParentEventIds(),
            traceState.getParentToChildrenEventIds(), traceState.getEventMap());

    // verify for all entries in the map, key is exit span and value is entry api boundary
    exitSpanToApiEntrySpanMap.forEach((key, value) -> {
      EnrichedSpanUtils.isExitSpan(traceState.getEventMap().get(key));
      EnrichedSpanUtils.isEntryApiBoundary(value);
    });
  }
}
