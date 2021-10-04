package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_DOT_AGENT;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SYSTEM;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_USER_AGENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.sf.uadetector.internal.data.domain.OperatingSystemPattern;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.grpc.Grpc;
import org.hypertrace.core.datamodel.eventfields.grpc.RequestMetadata;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.eventfields.http.RequestHeaders;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserAgentSpanEnricherTest extends AbstractAttributeEnricherTest {

  private static final Logger LOG = LoggerFactory.getLogger(UserAgentSpanEnricherTest.class);
  private UserAgentSpanEnricher enricher;

  @BeforeEach
  public void setUp() {
    enricher = new UserAgentSpanEnricher();
    enricher.init(mock(Config.class), mock(ClientRegistry.class));
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
      addAttribute(e, RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), "");

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
      addAttribute(e, RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), "");

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
          .thenReturn(
              Grpc.newBuilder()
                  .setRequest(
                      org.hypertrace.core.datamodel.eventfields.grpc.Request.newBuilder().build())
                  .build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // no user agent present
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_GRPC);
      when(e.getGrpc())
          .thenReturn(
              Grpc.newBuilder()
                  .setRequest(
                      org.hypertrace.core.datamodel.eventfields.grpc.Request.newBuilder()
                          .setRequestMetadata(RequestMetadata.newBuilder().build())
                          .build())
                  .build());
      enricher.enrichEvent(null, e);
      // Verify no enriched attributes are added
      assertEquals(1, e.getEnrichedAttributes().getAttributeMap().size());
    }

    // empty user agent on headers
    {
      Event e = createMockEvent();
      mockProtocol(e, Protocol.PROTOCOL_GRPC);
      when(e.getGrpc())
          .thenReturn(
              Grpc.newBuilder()
                  .setRequest(
                      org.hypertrace.core.datamodel.eventfields.grpc.Request.newBuilder()
                          .setRequestMetadata(RequestMetadata.newBuilder().setUserAgent("").build())
                          .build())
                  .build());
      addAttribute(e, RPC_REQUEST_METADATA_USER_AGENT.getValue(), "");

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
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), userAgent);
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), userAgent);

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
  public void enrichFromLongUserAgentString() {
    final String userAgent =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82"
            + " Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT"
            + " 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
            + " Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows"
            + " NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
            + " Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT"
            + " 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0"
            + " (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
            + " Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82"
            + " Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT"
            + " 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 "
            + "(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82"
            + " Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows"
            + " NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
            + " Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82"
            + " Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0"
            + " (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
            + " Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82"
            + " Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0"
            + " (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82"
            + " Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0"
            + " (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82"
            + " Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0"
            + " (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82"
            + " Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0"
            + " (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82"
            + " Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0"
            + " (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82"
            + " Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0"
            + " (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82"
            + " Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            + " Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            + " (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36";
    Set<OperatingSystemPattern> operatingSystemPatterns = new TreeSet<>();

    operatingSystemPatterns.add(newPattern(141, "^Mozilla.*Windows Phone 8.1", 1));
    operatingSystemPatterns.add(newPattern(35, "palm", 2));
    operatingSystemPatterns.add(newPattern(8, "Win 9x 4\\.90", 3));
    operatingSystemPatterns.add(newPattern(53, "MorphOS", 4));
    operatingSystemPatterns.add(newPattern(118, "iPhone OS 5_[0-9_]+", 5));
    operatingSystemPatterns.add(newPattern(117, "iPhone OS 4_[0-9_]+", 6));
    operatingSystemPatterns.add(newPattern(118, "iPad.*OS 5_[0-9_]+", 7));
    operatingSystemPatterns.add(newPattern(121, "iPad.*OS 6_[0-9_]+", 8));
    operatingSystemPatterns.add(newPattern(121, "iPhone OS 6_[0-9_]+", 9));
    operatingSystemPatterns.add(newPattern(126, "windows nt 6\\.2.*ARM", 10));
    operatingSystemPatterns.add(newPattern(129, "iPhone OS 7_[0-9_]+", 11));
    operatingSystemPatterns.add(newPattern(129, "iPad.*OS 7_[0-9_]+", 12));
    operatingSystemPatterns.add(newPattern(143, "iPad.*OS 8_[0-9_]+", 13));
    operatingSystemPatterns.add(newPattern(143, "iPhone OS 8_[0-9_]+", 14));
    operatingSystemPatterns.add(newPattern(146, "iPhone OS 3_[0-9_]+", 15));
    operatingSystemPatterns.add(newPattern(65, "iPhone OS ([0-9_]+) like Mac OS X", 16));
    operatingSystemPatterns.add(newPattern(29, "Solaris", 17));
    operatingSystemPatterns.add(newPattern(65, "iPhone OS 2_0", 18));
    operatingSystemPatterns.add(newPattern(34, "Series60", 19));
    operatingSystemPatterns.add(newPattern(65, "iPhone.*like Mac OS X", 20));
    operatingSystemPatterns.add(newPattern(96, "^HTC_HD2.*Opera.*windows", 21));
    operatingSystemPatterns.add(newPattern(123, "^Mozilla.*MSIE.*Windows NT 6.1.* Xbox", 22));
    operatingSystemPatterns.add(newPattern(127, "^Mozilla.*Windows Phone 8.0", 23));
    operatingSystemPatterns.add(
        newPattern(137, "^Mozilla\\/.*Ubuntu.*[Tablet|Mobile].*WebKit", 24));
    operatingSystemPatterns.add(newPattern(120, "Android 4\\.1", 25));
    operatingSystemPatterns.add(newPattern(124, "Android 4\\.2", 26));
    operatingSystemPatterns.add(newPattern(131, "Android 4\\.3", 27));
    operatingSystemPatterns.add(newPattern(136, "Android 4\\.4", 28));
    operatingSystemPatterns.add(newPattern(50, "BlackBerry", 29));
    operatingSystemPatterns.add(newPattern(70, "BeOS.*Haiku BePC", 30));
    operatingSystemPatterns.add(newPattern(96, "^Mozilla.*MSIE.*Windows.* XBLWP7", 31));
    operatingSystemPatterns.add(newPattern(96, "Windows Phone OS 7", 32));
    operatingSystemPatterns.add(newPattern(110, "Android 1.0", 33));
    operatingSystemPatterns.add(newPattern(103, "Android 1.5", 34));
    operatingSystemPatterns.add(newPattern(104, "Android 1.6", 35));
    operatingSystemPatterns.add(newPattern(105, "Android 2.0|Android 2.1", 36));
    operatingSystemPatterns.add(newPattern(106, "Android 2.2", 37));
    operatingSystemPatterns.add(newPattern(107, "Android 2.3|Android 2.4", 38));
    operatingSystemPatterns.add(newPattern(108, "Android 3.", 39));
    operatingSystemPatterns.add(newPattern(104, "Android Donut", 40));
    operatingSystemPatterns.add(newPattern(105, "Android Eclair", 41));
    operatingSystemPatterns.add(newPattern(111, "Android 4.", 42));
    operatingSystemPatterns.add(newPattern(113, "^Mozilla.*Tizen\\/1", 43));
    operatingSystemPatterns.add(newPattern(111, "Android-4.", 44));
    operatingSystemPatterns.add(newPattern(108, "Android\\/3", 45));
    operatingSystemPatterns.add(newPattern(134, "^Mozilla.*Tizen 2", 46));
    operatingSystemPatterns.add(newPattern(88, "(Windows Mobile)|(Windows Phone)", 47));
    operatingSystemPatterns.add(newPattern(1, "windows nt 5\\.1", 48));
    operatingSystemPatterns.add(newPattern(4, ".*windows 95.*", 49));
    operatingSystemPatterns.add(newPattern(2, "windows nt 5\\.0", 50));
    operatingSystemPatterns.add(newPattern(3, ".*windows nt 5\\.2( |;).*", 51));
    operatingSystemPatterns.add(newPattern(34, "Series80\\/2\\.0", 52));
    operatingSystemPatterns.add(newPattern(4, ".*win95.*", 53));
    operatingSystemPatterns.add(newPattern(5, "windows 98", 54));
    operatingSystemPatterns.add(newPattern(6, ".*win16( |;).*", 55));
    operatingSystemPatterns.add(newPattern(5, ".*win98( |;).*", 56));
    operatingSystemPatterns.add(newPattern(5, ".*windows 4\\.10( |;).*", 57));
    operatingSystemPatterns.add(newPattern(7, "windows ce|PocketPC", 58));
    operatingSystemPatterns.add(newPattern(8, ".*windows me( |;).*", 59));
    operatingSystemPatterns.add(newPattern(9, ".*windows nt 6\\.0( |;).*", 60));
    operatingSystemPatterns.add(newPattern(10, "j2me", 61));
    operatingSystemPatterns.add(newPattern(11, "centos", 62));
    operatingSystemPatterns.add(newPattern(12, "ubuntu", 63));
    operatingSystemPatterns.add(newPattern(13, "linux.*debian", 64));
    operatingSystemPatterns.add(newPattern(14, "linux.*fedora", 65));
    operatingSystemPatterns.add(newPattern(15, "linux.*gentoo", 66));
    operatingSystemPatterns.add(newPattern(16, "linux.*linspire", 67));
    operatingSystemPatterns.add(newPattern(17, "linux.*mandriva", 68));
    operatingSystemPatterns.add(newPattern(17, "linux.*mdk", 69));
    operatingSystemPatterns.add(newPattern(18, "linux.*redhat", 70));
    operatingSystemPatterns.add(newPattern(20, "linux.*slackware", 71));
    operatingSystemPatterns.add(newPattern(21, "linux.*kanotix", 72));
    operatingSystemPatterns.add(newPattern(22, "linux.*suse", 73));
    operatingSystemPatterns.add(newPattern(23, "linux.*knoppix", 74));
    operatingSystemPatterns.add(newPattern(24, ".*netbsd.*", 75));
    operatingSystemPatterns.add(newPattern(25, ".*freebsd.*", 76));
    operatingSystemPatterns.add(newPattern(26, ".*openbsd.*", 77));
    operatingSystemPatterns.add(newPattern(85, "Mac OS X (10_6|10\\.6)", 78));
    operatingSystemPatterns.add(newPattern(29, "sunos", 79));
    operatingSystemPatterns.add(newPattern(30, "amiga", 80));
    operatingSystemPatterns.add(newPattern(31, "irix", 81));
    operatingSystemPatterns.add(newPattern(32, "open.*vms", 82));
    operatingSystemPatterns.add(newPattern(33, "beos", 83));
    operatingSystemPatterns.add(newPattern(37, "webtv", 84));
    operatingSystemPatterns.add(newPattern(39, "os\\/2.*warp", 85));
    operatingSystemPatterns.add(newPattern(40, "RISC.OS", 86));
    operatingSystemPatterns.add(newPattern(41, "hp-ux", 87));
    operatingSystemPatterns.add(newPattern(46, "winnt", 88));
    operatingSystemPatterns.add(newPattern(34, "SonyEricssonP900", 89));
    operatingSystemPatterns.add(newPattern(49, "plan 9", 90));
    operatingSystemPatterns.add(newPattern(10, "NetFront.*Profile\\/MIDP", 91));
    operatingSystemPatterns.add(newPattern(34, "Series90.*Nokia7710", 92));
    operatingSystemPatterns.add(newPattern(20, "linux.*\\(Dropline GNOME\\).*", 93));
    operatingSystemPatterns.add(newPattern(46, "WinNT4\\.0", 94));
    operatingSystemPatterns.add(newPattern(18, "linux.*red hat", 95));
    operatingSystemPatterns.add(newPattern(52, "QNX x86pc", 96));
    operatingSystemPatterns.add(newPattern(18, "Red Hat modified", 97));
    operatingSystemPatterns.add(newPattern(46, "Windows\\-NT", 98));
    operatingSystemPatterns.add(newPattern(65, "iPad.*OS.*like Mac OS X", 99));
    operatingSystemPatterns.add(newPattern(2, "CYGWIN_NT\\-5.0", 100));
    operatingSystemPatterns.add(newPattern(34, "^DoCoMo.*F900i", 101));
    operatingSystemPatterns.add(newPattern(55, "Vector Linux", 102));
    operatingSystemPatterns.add(newPattern(40, "riscos", 103));
    operatingSystemPatterns.add(newPattern(56, "Linux Mint", 104));
    operatingSystemPatterns.add(newPattern(57, "SCO_SV", 105));
    operatingSystemPatterns.add(newPattern(22, "suse\\-linux", 106));
    operatingSystemPatterns.add(newPattern(58, "Arch Linux ([0-9a-zA-Z\\.\\-]+)", 107));
    operatingSystemPatterns.add(newPattern(59, "SkyOS", 108));
    operatingSystemPatterns.add(newPattern(6, ".*windows 3\\.1.*", 109));
    operatingSystemPatterns.add(newPattern(62, "Android ([0-9\\.]+)", 110));
    operatingSystemPatterns.add(newPattern(64, "windows nt 6\\.1", 111));
    operatingSystemPatterns.add(newPattern(2, ".*windows 2000( |;).*", 112));
    operatingSystemPatterns.add(newPattern(84, "Mac OS X (10_5|10\\.5)", 113));
    operatingSystemPatterns.add(newPattern(69, "webOS\\/.*AppleWebKit", 114));
    operatingSystemPatterns.add(newPattern(9, "Windows NT 6\\.0", 115));
    operatingSystemPatterns.add(newPattern(83, "Mac OS X (10_4|10\\.4)", 116));
    operatingSystemPatterns.add(newPattern(72, "Danger hiptop [0-9\\.]+", 117));
    operatingSystemPatterns.add(newPattern(1, "Windows_XP\\/5.1", 118));
    operatingSystemPatterns.add(newPattern(34, "SymbianOS", 119));
    operatingSystemPatterns.add(newPattern(64, ".*windows 7.*", 120));
    operatingSystemPatterns.add(newPattern(65, "iPhone OS [0-9\\.]+", 121));
    operatingSystemPatterns.add(newPattern(75, "Mozilla.*Linux.*Maemo", 122));
    operatingSystemPatterns.add(newPattern(34, "S60; SymbOS", 123));
    operatingSystemPatterns.add(newPattern(92, "PCLinuxOS\\/([0-9a-z\\.\\-]+)", 124));
    operatingSystemPatterns.add(newPattern(93, "^Mozilla\\/.*Linux.*Jolicloud", 125));
    operatingSystemPatterns.add(newPattern(94, "PLAYSTATION 3", 126));
    operatingSystemPatterns.add(newPattern(97, "^Mozilla.*CrOS.*Chrome", 127));
    operatingSystemPatterns.add(newPattern(62, "Android.*Linux.*Opera Mobi", 128));
    operatingSystemPatterns.add(newPattern(100, "windows nt 6\\.2", 129));
    operatingSystemPatterns.add(newPattern(101, "RIM Tablet OS 1[0-9\\.]+", 130));
    operatingSystemPatterns.add(newPattern(62, "Android", 131));
    operatingSystemPatterns.add(newPattern(102, "Bada\\/[0-9\\.]+", 132));
    operatingSystemPatterns.add(newPattern(112, "Mac OS X (10_7|10\\.7)", 133));
    operatingSystemPatterns.add(newPattern(69, "Linux.*hpwOS", 134));
    operatingSystemPatterns.add(newPattern(115, "^Mozilla.*Charon.*Inferno", 135));
    operatingSystemPatterns.add(newPattern(116, "Mac OS X (10_8|10\\.8)", 136));
    operatingSystemPatterns.add(newPattern(9, ".*Windows\\-Vista", 137));
    operatingSystemPatterns.add(newPattern(119, "RIM Tablet OS 2[0-9\\.]+", 138));
    operatingSystemPatterns.add(newPattern(122, "PlayStation Vita", 139));
    operatingSystemPatterns.add(newPattern(123, "^XBMC.*Xbox.*www\\.xbmc\\.org", 140));
    operatingSystemPatterns.add(
        newPattern(
            125,
            "^Mozilla\\/5\\.0 \\((Mobile|Tablet).*rv:[0-9\\.]+.*\\) Gecko\\/[0-9\\.]+ Firefox\\/[0-9\\.]+$",
            141));
    operatingSystemPatterns.add(newPattern(128, "Linux.*Mageia", 142));
    operatingSystemPatterns.add(newPattern(130, "Windows NT 6\\.3", 143));
    operatingSystemPatterns.add(newPattern(132, "Mac OS X (10_9|10\\.9)", 144));
    operatingSystemPatterns.add(newPattern(19, "Samsung.*SmartTV", 145));
    operatingSystemPatterns.add(newPattern(86, "AppleTV", 146));
    operatingSystemPatterns.add(newPattern(55, "VectorLinux", 147));
    operatingSystemPatterns.add(newPattern(133, "^Mozilla.*Nintendo 3DS", 148));
    operatingSystemPatterns.add(newPattern(50, "^Mozilla.*BB10.*Touch.*AppleWebKit.*Mobile", 149));
    operatingSystemPatterns.add(
        newPattern(62, "^Mozilla.*Linux.*AppleWebKit.*Puffin\\/[0-9\\.]+AT|AP$", 150));
    operatingSystemPatterns.add(
        newPattern(65, "^Mozilla.*Linux.*AppleWebKit.*Puffin\\/[0-9\\.]+IT|IP$", 151));
    operatingSystemPatterns.add(newPattern(138, "^Mozilla.*PlayStation 4.*AppleWebKit", 152));
    operatingSystemPatterns.add(newPattern(98, "Mozilla.*compatible.*Nitro.*Opera", 153));
    operatingSystemPatterns.add(newPattern(139, "Linux.*Jolla.*Sailfish", 154));
    operatingSystemPatterns.add(newPattern(128, "Mageia.*Linux", 155));
    operatingSystemPatterns.add(newPattern(140, "Nintendo.WiiU", 156));
    operatingSystemPatterns.add(newPattern(50, "^Mozilla.*BB10.*Kbd.*AppleWebKit.*Mobile", 157));
    operatingSystemPatterns.add(newPattern(142, "Mac OS X (10_10|10\\.10)", 158));
    operatingSystemPatterns.add(newPattern(102, "^Opera.*Bada", 159));
    operatingSystemPatterns.add(newPattern(144, "meego", 160));
    operatingSystemPatterns.add(
        newPattern(121, ".*\\/.*CFNetwork\\/(602|609|609\\.1\\.4) Darwin\\/", 161));
    operatingSystemPatterns.add(
        newPattern(
            129,
            ".*\\/.*CFNetwork\\/(672\\.0\\.2|672\\.0\\.8|672\\.1\\.12|672\\.1\\.13|672\\.1\\.14|672\\.1\\.15) Darwin\\/",
            162));
    operatingSystemPatterns.add(
        newPattern(
            117,
            ".*\\/.*CFNetwork\\/(485\\.2|485\\.10\\.2|485\\.12\\.7|485\\.12\\.30|485\\.13\\.9) Darwin\\/",
            163));
    operatingSystemPatterns.add(
        newPattern(118, ".*\\/.*CFNetwork\\/(548\\.0\\.3|548\\.0\\.4|548\\.1\\.4) Darwin\\/", 164));
    operatingSystemPatterns.add(newPattern(146, ".*\\/.*CFNetwork\\/459 Darwin\\/", 165));
    operatingSystemPatterns.add(
        newPattern(90, ".*\\/.*CFNetwork\\/(1\\.2\\.1|1\\.2\\.2|1\\.2\\.6) Darwin\\/", 166));
    operatingSystemPatterns.add(
        newPattern(
            83,
            ".*\\/.*CFNetwork\\/(128|128\\.2|129\\.5|129\\.9|129\\.10|129\\.13|129\\.16|129\\.18|129\\.20|129\\.21|129\\.22) Darwin\\/",
            167));
    operatingSystemPatterns.add(
        newPattern(
            84,
            ".*\\/.*CFNetwork\\/(217|220|221\\.5|330|330\\.4|339\\.5|422\\.11|438\\.12|438\\.14) Darwin\\/",
            168));
    operatingSystemPatterns.add(
        newPattern(
            85,
            ".*\\/.*CFNetwork\\/(454\\.4|454\\.5|454\\.9\\.4|454\\.9\\.7|454\\.11\\.5|454\\.11\\.12|454\\.12\\.4) Darwin\\/",
            169));
    operatingSystemPatterns.add(
        newPattern(
            112,
            ".*\\/.*CFNetwork\\/(520\\.0\\.13|520\\.2\\.5|520\\.3\\.2|520\\.4\\.3|520\\.5\\.1) Darwin\\/",
            170));
    operatingSystemPatterns.add(
        newPattern(
            116,
            ".*\\/.*CFNetwork\\/(596\\.0\\.1|596\\.1|596\\.2\\.3|596\\.3\\.3|596\\.4\\.3|596\\.5) Darwin\\/",
            171));
    operatingSystemPatterns.add(
        newPattern(
            132, ".*\\/.*CFNetwork\\/(673\\.0\\.3|673\\.2\\.1|673\\.3|673\\.4) Darwin\\/", 172));
    operatingSystemPatterns.add(newPattern(142, ".*\\/.*CFNetwork\\/703\\.1 Darwin\\/", 173));
    operatingSystemPatterns.add(newPattern(86, "Mac OS X", 174));
    operatingSystemPatterns.add(newPattern(19, "linux", 175));
    operatingSystemPatterns.add(newPattern(42, "Nintendo.Wii", 176));
    operatingSystemPatterns.add(newPattern(15, "Gentoo i686", 177));
    operatingSystemPatterns.add(newPattern(22, "Konqueror.*SUSE", 178));
    operatingSystemPatterns.add(newPattern(14, "Konqueror.*Fedora", 179));
    operatingSystemPatterns.add(newPattern(10, "Obigo.*MIDP", 180));
    operatingSystemPatterns.add(newPattern(10, "Teleca.*MIDP", 181));
    operatingSystemPatterns.add(newPattern(2, "Windows 2000", 182));
    operatingSystemPatterns.add(newPattern(46, "Windows NT 4", 183));
    operatingSystemPatterns.add(newPattern(34, "symbian", 184));
    operatingSystemPatterns.add(newPattern(87, "os\\/2", 185));
    operatingSystemPatterns.add(newPattern(1, ".*windows XP.*", 186));
    operatingSystemPatterns.add(newPattern(61, ".*dragonfly.*", 187));
    operatingSystemPatterns.add(newPattern(8, "Windows ME", 188));
    operatingSystemPatterns.add(newPattern(34, "NokiaN70", 189));
    operatingSystemPatterns.add(newPattern(46, "NT4\\.0", 190));
    operatingSystemPatterns.add(newPattern(94, "PlayStation Portable", 191));
    operatingSystemPatterns.add(newPattern(43, "windows", 192));
    operatingSystemPatterns.add(newPattern(44, "mac_powerpc", 193));
    operatingSystemPatterns.add(newPattern(44, "Macintosh", 194));
    operatingSystemPatterns.add(newPattern(45, "aix", 195));
    operatingSystemPatterns.add(newPattern(43, "Win32", 196));
    operatingSystemPatterns.add(newPattern(47, "java\\/[0-9a-z\\.]+", 197));
    operatingSystemPatterns.add(newPattern(74, "Syllable", 198));
    operatingSystemPatterns.add(newPattern(44, "powerpc\\-apple", 199));
    operatingSystemPatterns.add(newPattern(95, "AROS", 200));
    operatingSystemPatterns.add(newPattern(47, "java[0-9a-z\\.]+", 201));
    operatingSystemPatterns.add(newPattern(34, "Series 60", 202));
    operatingSystemPatterns.add(newPattern(44, "os=Mac", 203));
    operatingSystemPatterns.add(newPattern(44, "SO=MAC10,6", 204));
    operatingSystemPatterns.add(newPattern(44, "so=Mac 10.5.8", 205));
    operatingSystemPatterns.add(newPattern(91, "Minix 3", 206));
    operatingSystemPatterns.add(newPattern(98, "Nintendo DS", 207));
    operatingSystemPatterns.add(newPattern(62, "^Opera.*Android", 208));
    operatingSystemPatterns.add(newPattern(34, "NokiaN97", 209));
    operatingSystemPatterns.add(newPattern(34, "Nokia.*XpressMusic", 210));
    operatingSystemPatterns.add(newPattern(34, "NokiaE66", 211));
    operatingSystemPatterns.add(newPattern(34, "Nokia6700", 212));
    operatingSystemPatterns.add(newPattern(99, "\\(GNU;", 213));
    operatingSystemPatterns.add(newPattern(145, "brew", 214));
    operatingSystemPatterns.add(newPattern(44, "macos", 215));
    operatingSystemPatterns.add(newPattern(90, "Darwin 10\\.3", 216));
    operatingSystemPatterns.add(newPattern(65, "iPhone", 217));
    operatingSystemPatterns.add(newPattern(19, "Unix", 218));
    operatingSystemPatterns.add(newPattern(44, "Darwin", 219));

    for (OperatingSystemPattern operatingSystemPattern : operatingSystemPatterns) {
      Matcher matcher = operatingSystemPattern.getPattern().matcher(userAgent);
      long start = System.currentTimeMillis();
      if (matcher.find()) {
        long end = System.currentTimeMillis();
        LOG.info(
            "not found. pattern: {},  time taken: {}",
            operatingSystemPattern.getPattern(),
            end - start);
        break;
      }
      long end = System.currentTimeMillis();
      if (end - start > 10) {
        LOG.info(
            "not found. pattern: {},  time taken: {}",
            operatingSystemPattern.getPattern(),
            end - start);
      }
    }

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
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), userAgent);
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), userAgent);

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
        "Windows",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_NAME)).getValue());
    assertEquals(
        "10.0",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_OS_VERSION)).getValue());
    assertEquals(
        "93.0.4577.82",
        map.get(Constants.getEnrichedSpanConstant(UserAgent.USER_AGENT_BROWSER_VERSION))
            .getValue());
  }

  OperatingSystemPattern newPattern(int id, String pattern, int position) {
    return new OperatingSystemPattern(id, Pattern.compile(pattern), position);
  }

  @Test
  public void enrichUserAgentFromGrpc() {
    String userAgent = "grpc-java-okhttp/1.19.0";

    Event e = createMockEvent();
    mockProtocol(e, Protocol.PROTOCOL_GRPC);
    addAttribute(e, RPC_REQUEST_METADATA_USER_AGENT.getValue(), userAgent);
    addAttribute(e, OTEL_SPAN_TAG_RPC_SYSTEM.getValue(), "grpc");
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
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), userAgent);
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), userAgent);

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
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), userAgent);
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), userAgent);

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
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), badUserAgent);
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), badUserAgent);

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
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), "I am not a User Agent!");
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER), userAgent);

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
                .setRequest(Request.newBuilder().setUserAgent(userAgent).build())
                .build());
    mockProtocol(e, Protocol.PROTOCOL_HTTP);
    addAttribute(e, RawSpanConstants.getValue(HTTP_USER_DOT_AGENT), userAgent);

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
    event
        .getEnrichedAttributes()
        .getAttributeMap()
        .put(
            Constants.getEnrichedSpanConstant(CommonAttribute.COMMON_ATTRIBUTE_PROTOCOL),
            AttributeValue.newBuilder()
                .setValue(Constants.getEnrichedSpanConstant(protocol))
                .build());
  }

  private void addAttribute(Event event, String key, String val) {
    event
        .getAttributes()
        .getAttributeMap()
        .put(key, AttributeValue.newBuilder().setValue(val).build());
  }
}
