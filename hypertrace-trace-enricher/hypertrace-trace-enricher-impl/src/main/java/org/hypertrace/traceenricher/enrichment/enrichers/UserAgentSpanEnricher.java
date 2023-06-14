package org.hypertrace.traceenricher.enrichment.enrichers;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.Optional;
import net.sf.uadetector.ReadableUserAgent;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.util.UserAgentParser;

public class UserAgentSpanEnricher extends AbstractTraceEnricher {

  private UserAgentParser userAgentParser;

  @Override
  public void init(Config enricherConfig, ClientRegistry clientRegistry) {
    this.userAgentParser = clientRegistry.getUserAgentParser();
  }

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
    Optional<ReadableUserAgent> maybeUserAgent = this.userAgentParser.getUserAgent(event);
    maybeUserAgent.ifPresent(
        userAgent -> {
          addEnrichedAttribute(
              event,
              EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_NAME),
              AttributeValueCreator.create(userAgent.getName()));
          addEnrichedAttribute(
              event,
              EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_TYPE),
              AttributeValueCreator.create(userAgent.getType().getName()));
          addEnrichedAttribute(
              event,
              EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_DEVICE_CATEGORY),
              AttributeValueCreator.create(userAgent.getDeviceCategory().getName()));
          addEnrichedAttribute(
              event,
              EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_OS_NAME),
              AttributeValueCreator.create(userAgent.getOperatingSystem().getName()));
          addEnrichedAttribute(
              event,
              EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_OS_VERSION),
              AttributeValueCreator.create(
                  userAgent.getOperatingSystem().getVersionNumber().toVersionString()));
          addEnrichedAttribute(
              event,
              EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_BROWSER_VERSION),
              AttributeValueCreator.create(userAgent.getVersionNumber().toVersionString()));
        });
  }
}
