package org.hypertrace.traceenricher.enrichment.enrichers;

import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.grpc.Grpc;
import org.hypertrace.core.datamodel.eventfields.grpc.RequestMetadata;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.eventfields.http.RequestHeaders;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

public class UserAgentSpanEnricherTest extends AbstractAttributeEnricherTest {

  private UserAgentSpanEnricher enricher;

  @BeforeEach
  public void setUp() {
    enricher = new UserAgentSpanEnricher();
  }

  @Test
  public void noUserAgent() {
    // no http present
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_HTTP);
      when(e.getHttp()).thenReturn(null);
      when(e.getGrpc()).thenReturn(null);
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // no request present for http
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_HTTP);
      when(e.getHttp()).thenReturn(Http.newBuilder().build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // no request headers and user agent present for request
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_HTTP);
      when(e.getHttp())
          .thenReturn(Http.newBuilder().setRequest(Request.newBuilder().build()).build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // no user agent present
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_HTTP);
      when(e.getHttp())
          .thenReturn(
              Http.newBuilder()
                  .setRequest(
                      Request.newBuilder().setHeaders(RequestHeaders.newBuilder().build()).build())
                  .build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // empty user agent on headers
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_HTTP);
      when(e.getHttp())
          .thenReturn(
              Http.newBuilder()
                  .setRequest(
                      Request.newBuilder()
                          .setHeaders(RequestHeaders.newBuilder().setUserAgent("").build())
                          .build())
                  .build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // empty user agent on request
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_HTTP);
      when(e.getHttp())
          .thenReturn(
              Http.newBuilder().setRequest(Request.newBuilder().setUserAgent("").build()).build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }



    // no request present for http
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_GRPC);
      when(e.getGrpc()).thenReturn(Grpc.newBuilder().build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // no request headers and user agent present for request
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_GRPC);
      when(e.getGrpc())
          .thenReturn(Grpc.newBuilder().setRequest(
              org.hypertrace.core.datamodel.eventfields.grpc.Request.newBuilder().build()
          ).build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // no user agent present
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_GRPC);
      when(e.getGrpc())
          .thenReturn(Grpc.newBuilder().setRequest(
              org.hypertrace.core.datamodel.eventfields.grpc.Request.newBuilder()
                  .setRequestMetadata(
                      RequestMetadata.newBuilder().build()
                  ).build()
          ).build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // empty user agent on headers
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_GRPC);
      when(e.getGrpc())
          .thenReturn(Grpc.newBuilder().setRequest(
              org.hypertrace.core.datamodel.eventfields.grpc.Request.newBuilder()
                  .setRequestMetadata(
                      RequestMetadata.newBuilder().setUserAgent("").build()
                  ).build()
          ).build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }
  }

  @Test
  public void enrichFromUserAgent() {
    String userAgent =
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36";

    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setHeaders(RequestHeaders.newBuilder().setUserAgent(userAgent).build())
                        .setUserAgent(userAgent)
                        .build())
                .build());
    mockProtocol(e, Protocol.PROTOCOL_HTTP);
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(7, map.size());
    assertEquals(
        "Chrome", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "Browser",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_TYPE)).getValue());
    assertEquals(
        "Personal computer",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "OS X",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "10.14.3",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "73.0.3683.103",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  @Test
  public void enrichUserAgentFromGrpc() {
    String userAgent = "grpc-java-okhttp/1.19.0";

    Event e = createMockEvent();
    when(e.getGrpc())
        .thenReturn(
            Grpc.newBuilder().setRequest(
                org.hypertrace.core.datamodel.eventfields.grpc.Request.newBuilder().setRequestMetadata(
                    RequestMetadata.newBuilder().setUserAgent(userAgent).build()
                ).build()
            ).build()
        );
    mockProtocol(e, Protocol.PROTOCOL_GRPC);
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(7, map.size());
    assertEquals(
        "unknown",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "unknown",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  @Test
  public void badOS() {
    String userAgent =
        "Mozilla/5.0"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36";

    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setHeaders(RequestHeaders.newBuilder().setUserAgent(userAgent).build())
                        .setUserAgent(userAgent)
                        .build())
                .build());
    mockProtocol(e, Protocol.PROTOCOL_HTTP);
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(7, map.size());
    assertEquals(
        "Chrome", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "Browser",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_TYPE)).getValue());
    assertEquals(
        "Personal computer",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "unknown",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "73.0.3683.103",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  @Test
  public void badBrowser() {
    String userAgent =
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chroma/73.0.3683.103";

    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setHeaders(RequestHeaders.newBuilder().setUserAgent(userAgent).build())
                        .setUserAgent(userAgent)
                        .build())
                .build());
    mockProtocol(e, Protocol.PROTOCOL_HTTPS);
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(7, map.size());
    assertEquals(
        "unknown",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "OS X",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "10.14.3",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  @Test
  public void badUserAgent() {
    String badUserAgent = "I am not a User Agent!";
    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setHeaders(RequestHeaders.newBuilder().setUserAgent(badUserAgent).build())
                        .setUserAgent(badUserAgent)
                        .build())
                .build());
    mockProtocol(e, Protocol.PROTOCOL_HTTP);
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(7, map.size());
    assertEquals(
        "unknown",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "unknown",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  @Test
  public void should_preferUserAgent_fromHeaders() {
    String userAgent =
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36";

    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setHeaders(RequestHeaders.newBuilder().setUserAgent(userAgent).build())
                        .setUserAgent("I am not a User Agent!")
                        .build())
                .build());
    mockProtocol(e, Protocol.PROTOCOL_HTTP);
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(7, map.size());
    assertEquals(
        "Chrome", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "Browser",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_TYPE)).getValue());
    assertEquals(
        "Personal computer",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "OS X",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "10.14.3",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "73.0.3683.103",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  @Test
  public void should_fallbackToUserAgent_fromRequest() {
    String userAgent =
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36";

    Event e = createMockEvent();
    when(e.getHttp())
        .thenReturn(
            Http.newBuilder()
                .setRequest(
                    Request.newBuilder()
                        .setUserAgent(userAgent)
                        .build())
                .build());
    mockProtocol(e, Protocol.PROTOCOL_HTTP);
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(7, map.size());
    assertEquals(
        "Chrome", map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_NAME)).getValue());
    assertEquals(
        "Browser",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_TYPE)).getValue());
    assertEquals(
        "Personal computer",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_DEVICE_CATEGORY))
            .getValue());
    assertEquals(
        "OS X",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "10.14.3",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "73.0.3683.103",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  private void mockProtocol(Event event, Protocol protocol) {
    event.getEnrichedAttributes().getAttributeMap()
        .put(Constants.getEnrichedSpanConstant(CommonAttribute.COMMON_ATTRIBUTE_PROTOCOL),
            AttributeValue.newBuilder().setValue(Constants.getEnrichedSpanConstant(protocol)).build()
        );
  }
}
