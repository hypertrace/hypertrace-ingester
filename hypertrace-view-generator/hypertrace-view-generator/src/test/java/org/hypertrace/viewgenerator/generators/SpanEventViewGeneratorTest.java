package org.hypertrace.viewgenerator.generators;

import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
}
