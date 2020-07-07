package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import net.sf.uadetector.OperatingSystem;
import net.sf.uadetector.OperatingSystemFamily;
import net.sf.uadetector.ReadableDeviceCategory;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgent;
import net.sf.uadetector.VersionNumber;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UserAgentSpanEnricherTest extends AbstractAttributeEnricherTest {

  private static final String USER_AGENT_HEADER =
      UserAgentSpanEnricher.HTTP_REQUEST_HEADER_PREFIX + UserAgentSpanEnricher.USER_AGENT_HEADER;

  private UserAgentSpanEnricher enricher;

  @BeforeEach
  public void setUp() {
    enricher = new UserAgentSpanEnricher();
  }

  @Test
  public void noUserAgent() {
    Event e = mock(Event.class);
    when(e.getAttributes()).thenReturn(null);
    enricher.enrichEvent(null, e);

    when(e.getAttributes()).thenReturn(new Attributes());
    enricher.enrichEvent(null, e);

    e = createMockEvent();
    Map<String, AttributeValue> attributeValueMap = e.getAttributes().getAttributeMap();
    attributeValueMap.put("xyz", AttributeValueCreator.create("xyz"));
    enricher.enrichEvent(null, e);
    // Verify no enriched attributes are added
    assertEquals(0, e.getEnrichedAttributes().getAttributeMap().size());
  }

  @Test
  public void enrichFromUserAgent() {
    Event e = createMockEvent();

    String header = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3)" +
        " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36";

    Map<String, AttributeValue> attributeValueMap = e.getAttributes().getAttributeMap();
    attributeValueMap.put(USER_AGENT_HEADER, AttributeValueCreator.create(header));

    enricher.enrichEvent(null, e);

    ReadableUserAgent expected = UADetectorServiceFactory.getResourceModuleParser().parse(header);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(map.get(Constants.getEnrichedSpanConstant(
        org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_NAME)).getValue(),
        expected.getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_TYPE)).getValue(),
        expected.getType().getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_DEVICE_CATEGORY)).getValue(),
        expected.getDeviceCategory().getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_OS_NAME)).getValue(),
        expected.getOperatingSystem().getName());
  }

  @Test
  public void badOS() {
    String header = "Mozilla/5.0" +
        " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36";

    Event e = createMockEvent();

    Map<String, AttributeValue> attributeValueMap = e.getAttributes().getAttributeMap();
    attributeValueMap.put(
        UserAgentSpanEnricher.HTTP_REQUEST_HEADER_PREFIX + UserAgentSpanEnricher.USER_AGENT_HEADER,
        AttributeValueCreator.create(header));

    enricher.enrichEvent(null, e);

    ReadableUserAgent expected = UADetectorServiceFactory.getResourceModuleParser().parse(header);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_NAME)).getValue(),
        expected.getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_TYPE)).getValue(),
        expected.getType().getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_DEVICE_CATEGORY)).getValue(),
        expected.getDeviceCategory().getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_OS_NAME)).getValue(),
        expected.getOperatingSystem().getName());
  }

  @Test
  public void badBrowser() {
    UserAgentSpanEnricher enricher = new UserAgentSpanEnricher();

    Event e = createMockEvent();
    e.getAttributes().getAttributeMap().put(USER_AGENT_HEADER,
        AttributeValueCreator.create("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3)" +
            " AppleWebKit/537.36 (KHTML, like Gecko) Chroma/73.0.3683.103"));

    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(map.size(), 6);
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_NAME)).getValue(),
        UserAgent.EMPTY.getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_BROWSER_VERSION)).getValue(),
        VersionNumber.UNKNOWN.toVersionString());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_DEVICE_CATEGORY)).getValue(),
        ReadableDeviceCategory.Category.UNKNOWN.getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_OS_NAME)).getValue(),
        OperatingSystemFamily.OS_X.getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_OS_VERSION)).getValue(),
        "10.14.3");
  }

  @Test
  public void emptyUserAgent() {
    UserAgentSpanEnricher enricher = new UserAgentSpanEnricher();

    Event e = createMockEvent();
    Map<String, AttributeValue> map = e.getAttributes().getAttributeMap();
    map.put(USER_AGENT_HEADER, AttributeValueCreator.create(""));
    enricher.enrichEvent(null, e);

    assertEquals(1, map.size());
    assertNull(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_NAME)));
    assertNull(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_BROWSER_VERSION)));
    assertNull(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_DEVICE_CATEGORY)));
    assertNull(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_OS_NAME)));
    assertNull(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_OS_VERSION)));
  }

  @Test
  public void badUserAgent() {
    UserAgentSpanEnricher enricher = new UserAgentSpanEnricher();

    Event e = createMockEvent();
    e.getAttributes().getAttributeMap().put(USER_AGENT_HEADER,
        AttributeValueCreator.create("I am not a User Agent!"));
    enricher.enrichEvent(null, e);

    Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
    assertEquals(map.size(), 6);
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_NAME)).getValue(),
        UserAgent.EMPTY.getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_BROWSER_VERSION)).getValue(),
        VersionNumber.UNKNOWN.toVersionString());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_DEVICE_CATEGORY)).getValue(),
        ReadableDeviceCategory.Category.UNKNOWN.getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_OS_NAME)).getValue(),
        OperatingSystem.EMPTY.getName());
    assertEquals(map.get(Constants.getEnrichedSpanConstant(org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent.USER_AGENT_OS_VERSION)).getValue(),
        VersionNumber.UNKNOWN.toVersionString());
  }
}
