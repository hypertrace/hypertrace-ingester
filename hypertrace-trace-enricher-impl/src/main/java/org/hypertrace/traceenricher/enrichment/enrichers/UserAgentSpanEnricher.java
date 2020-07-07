package org.hypertrace.traceenricher.enrichment.enrichers;

import java.util.Map;
import java.util.Optional;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;

public class UserAgentSpanEnricher extends AbstractTraceEnricher {

  static final String USER_AGENT_HEADER = "user-agent";
  static final String HTTP_REQUEST_HEADER_PREFIX = "http.request.header.";
  private UserAgentStringParser userAgentStringParser =
      UADetectorServiceFactory.getResourceModuleParser();

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    if (event.getAttributes() == null) {
      return;
    }

    Map<String, AttributeValue> attributeMap = event.getAttributes().getAttributeMap();
    if (attributeMap == null) {
      return;
    }

    // extract the user-agent header
    Optional<String> userAgentHeader = attributeMap.keySet().stream()
        .filter(k -> k.equalsIgnoreCase(HTTP_REQUEST_HEADER_PREFIX + USER_AGENT_HEADER))
        .findFirst();

    if (userAgentHeader.isPresent()) {
      String value = attributeMap.get(userAgentHeader.get()).getValue();
      ReadableUserAgent userAgent = userAgentStringParser.parse(value);
      addEnrichedAttribute(event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_NAME),
          AttributeValueCreator.create(userAgent.getName()));
      addEnrichedAttribute(event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_TYPE),
          AttributeValueCreator.create(userAgent.getType().getName()));
      addEnrichedAttribute(event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_DEVICE_CATEGORY),
          AttributeValueCreator.create(userAgent.getDeviceCategory().getName()));
      addEnrichedAttribute(event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_OS_NAME),
          AttributeValueCreator.create(userAgent.getOperatingSystem().getName()));
      addEnrichedAttribute(event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_OS_VERSION),
          AttributeValueCreator.create(userAgent.getOperatingSystem().getVersionNumber().toVersionString()));
      addEnrichedAttribute(event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_BROWSER_VERSION),
          AttributeValueCreator.create(userAgent.getVersionNumber().toVersionString()));
    }
  }
}
