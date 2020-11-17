package org.hypertrace.traceenricher.enrichment.enrichers;

import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Envoy;
import org.hypertrace.core.span.constants.v1.OCAttribute;
import org.hypertrace.core.span.constants.v1.OCSpanKind;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.core.span.constants.v1.SpanNamePrefix;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.tagresolver.SpanTagResolver;
import org.hypertrace.traceenricher.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

/**
 * Enricher that figures out if an event is an entry event and by adding EVENT_TYPE attribute.
 * "EXIT" means calls going out. "ENTRY" means calls coming in or code executing in the server.
 * "UNKNOWN" means this enricher cannot conclusively figure out if this is exit or entry.
 */
public class SpanTypeAttributeEnricher extends AbstractTraceEnricher {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpanTypeAttributeEnricher.class);
  private static final String PROTOCOL_ATTR =
      EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_PROTOCOL);

  private static final String GRPC_PROTOCOL_VALUE =
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_GRPC);
  private static final String HTTP_PROTOCOL_VALUE =
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_HTTP);
  private static final Map<String, Protocol> NAME_TO_PROTOCOL_MAP = Map.of(
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_GRPC), Protocol.PROTOCOL_GRPC,
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_HTTP), Protocol.PROTOCOL_HTTP,
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_HTTPS), Protocol.PROTOCOL_HTTPS,
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_REDIS), Protocol.PROTOCOL_REDIS,
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_MONGO), Protocol.PROTOCOL_MONGO,
      Constants.getEnrichedSpanConstant(Protocol.PROTOCOL_JDBC), Protocol.PROTOCOL_JDBC
  );

  private final String spanTypeAttrName =
      EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE);

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    if (null == event.getAttributes() || null == event.getAttributes().getAttributeMap()) {
      return;
    }

    Boolean isEntry = SpanTagResolver.checkForEntrySpan(event);

    // Add the new information as an enriched attribute, not raw attribute.
    if (isEntry == null) {
      addEnrichedAttribute(event, spanTypeAttrName, AttributeValueCreator.create(
          Constants.getEnrichedSpanConstant(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_UNSPECIFIED)));
    } else if (isEntry) {
      addEnrichedAttribute(event, spanTypeAttrName, AttributeValueCreator.create(
          Constants.getEnrichedSpanConstant(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY)));
    } else {
      addEnrichedAttribute(event, spanTypeAttrName, AttributeValueCreator.create(
          Constants.getEnrichedSpanConstant(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_EXIT)));
    }

    // Get the protocol and name and create API entity based on the protocol.
    Protocol protocol = getProtocolName(event);
    addEnrichedAttribute(event, PROTOCOL_ATTR,
        AttributeValueCreator.create(Constants.getEnrichedSpanConstant(protocol)));
  }

  // todo: move the protocol logic also to grpc & http tag resolver?
  // todo: how to find protocol for OTEL?

  @Nonnull
  public static Protocol getProtocolName(Event event) {
    Protocol protocol = getGrpcProtocol(event);

    if (protocol == Protocol.PROTOCOL_GRPC) {
      return protocol;
    } else if (protocol == Protocol.PROTOCOL_UNSPECIFIED || protocol == Protocol.UNRECOGNIZED) {
      return getHttpProtocol(event);
    }

    return Protocol.PROTOCOL_UNSPECIFIED;
  }

  @Nonnull
  public static Protocol getGrpcProtocol(Event event) {
    Map<String, AttributeValue> attributeMap = event.getAttributes().getAttributeMap();

    if (event.getRpc() != null && event.getRpc().getSystem() != null) {
      String rpcSystem = event.getRpc().getSystem();
      if (GRPC_PROTOCOL_VALUE.equalsIgnoreCase(rpcSystem)) {
        return Protocol.PROTOCOL_GRPC;
      }
    }

    // check Open Tracing grpc component value first
    AttributeValue componentAttrValue = attributeMap.get(
        RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_COMPONENT));
    if (componentAttrValue != null) {
      if (GRPC_PROTOCOL_VALUE.equalsIgnoreCase(componentAttrValue.getValue())) {
        return Protocol.PROTOCOL_GRPC;
      }
    }

    /* This logic is the brute force checking if there's any attribute starts with grpc.
     * Unfortunately, depends on the language, instrumented vs non, we can't count on a set
     * of attributes that can identify the protocol.
     */
    for (String attrKey : attributeMap.keySet()) {
      String upperCaseKey = attrKey.toUpperCase();
      if (upperCaseKey.startsWith(GRPC_PROTOCOL_VALUE.toUpperCase())) {
        return Protocol.PROTOCOL_GRPC;
      }
    }

    // this means, there's no grpc prefix protocol
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Couldn't map the event to any protocol; eventId: {}", event.getEventId());
    }

    return Protocol.PROTOCOL_UNSPECIFIED;
  }

  @Nonnull
  public static Protocol getHttpProtocol(Event event) {
    Optional<String> scheme = Optional.ofNullable(event.getHttp()).map(Http::getRequest).map(Request::getScheme);
    if (scheme.isPresent()) {
      Protocol protocol = NAME_TO_PROTOCOL_MAP.get(scheme.get().toUpperCase());
      if (protocol != null) {
        return protocol;
      }
    }

    /* This logic is the brute force checking if there's all attribute starts with http.
     * As in, there shouldn't be any grpc attribute
     * Unfortunately, depends on the language, instrumented vs non, we can't count on a set
     * of attributes that can identify the protocol.
     */
    boolean hasHttpPrefix = false;
    // Go through all attributes check if there's GRPC attribute. If there are any grpc attribute,
    // then it's not a HTTP protocol
    for (String attrKey : event.getAttributes().getAttributeMap().keySet()) {
      String upperCaseKey = attrKey.toUpperCase();
      if (upperCaseKey.startsWith(HTTP_PROTOCOL_VALUE.toUpperCase())) {
        // just marking if http exists but do not decide on the protocol.
        // It needs to complete checking grpc before deciding
        hasHttpPrefix = true;
      } else if (upperCaseKey.startsWith(GRPC_PROTOCOL_VALUE.toUpperCase())) {
        // if any of the attribute starts with GRPC, it's not HTTP protocol
        return Protocol.PROTOCOL_UNSPECIFIED;
      }
    }

    // this means, there's no grpc prefix protocol, then check if there were HTTP
    if (hasHttpPrefix) {
      return Protocol.PROTOCOL_HTTP;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Couldn't map the event to any protocol; eventId: {}", event.getEventId());
    }

    return Protocol.PROTOCOL_UNSPECIFIED;
  }
}
