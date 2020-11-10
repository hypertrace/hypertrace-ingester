package org.hypertrace.traceenricher.enrichment.enrichers;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.eventfields.grpc.Request;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;

import java.util.Map;
import java.util.Optional;

public class UserAgentSpanEnricher extends AbstractTraceEnricher {

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
    Optional<String> mayBeUserAgent = getUserAgent(event);

    if (mayBeUserAgent.isPresent()) {
      ReadableUserAgent userAgent = userAgentStringParser.parse(mayBeUserAgent.get());
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

  private Optional<String> getUserAgent(Event event) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    if (Protocol.PROTOCOL_HTTP == protocol || Protocol.PROTOCOL_HTTPS == protocol) {
      if (event.getHttp() != null && event.getHttp().getRequest() != null) {
        // prefer user agent from headers
        if (event.getHttp().getRequest().getHeaders() != null
            && !StringUtils.isEmpty(event.getHttp().getRequest().getHeaders().getUserAgent())) {
          return Optional.of(event.getHttp().getRequest().getHeaders().getUserAgent());
        }

        // fallback to user agent on the request
        if (!StringUtils.isEmpty(event.getHttp().getRequest().getUserAgent())) {
          return Optional.of(event.getHttp().getRequest().getUserAgent());
        }
      }
    } else if (Protocol.PROTOCOL_GRPC == protocol) {
      if (event.getGrpc() != null && event.getGrpc().getRequest() != null) {
        Request request = event.getGrpc().getRequest();
        if (request.getRequestMetadata() != null
            && !StringUtils.isEmpty(request.getRequestMetadata().getUserAgent())) {
          return Optional.of(request.getRequestMetadata().getUserAgent());
        }
      }
    }

    return Optional.empty();
  }
}
