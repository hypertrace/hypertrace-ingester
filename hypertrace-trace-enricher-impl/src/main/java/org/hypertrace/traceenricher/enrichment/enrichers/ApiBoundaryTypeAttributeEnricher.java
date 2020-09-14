package org.hypertrace.traceenricher.enrichment.enrichers;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Http;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;

/**
 * This is to determine if the span is the entry / exit point for a particular API.
 * We can't use the Entry / Exit span kind to determine if it's true API entry/exit.
 */
public class ApiBoundaryTypeAttributeEnricher extends AbstractTraceEnricher {
  private static final String COLON = ":";
  private static final Splitter COLON_SPLITTER = Splitter.on(COLON);
  private static final String HOST_HEADER =
      EnrichedSpanConstants.getValue(Http.HTTP_HOST);
  private static final String API_BOUNDARY_TYPE_ATTR_NAME =
      EnrichedSpanConstants.getValue(Api.API_BOUNDARY_TYPE);
  private static final String ENTRY_BOUNDARY_TYPE =
      EnrichedSpanConstants.getValue(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY);
  private static final String EXIT_BOUNDARY_TYPE =
      EnrichedSpanConstants.getValue(BoundaryTypeValue.BOUNDARY_TYPE_VALUE_EXIT);
  private static final String X_FORWARDED_HOST_HEADER = "x-forwarded-host";

  private static final List<String> HOST_HEADER_ATTRIBUTES = ImmutableList.of(
      // The order of these constants is important because that enforces the priority for
      // different keys/headers.
      RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HOST_HEADER),
      RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_AUTHORITY_HEADER),
      RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_HOST),
      // In the cases where there are sidecar proxies, the host header might be set to localhost
      // while the original host will be moved to x-forwarded headers. Hence, read them too.
      X_FORWARDED_HOST_HEADER
  );
  private static final String LOCALHOST = "localhost";

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    if (event.getEnrichedAttributes() == null) {
      return;
    }

    Map<String, AttributeValue> attributeMap = event.getEnrichedAttributes().getAttributeMap();
    if (attributeMap == null) {
      return;
    }

    boolean isEntrySpan = EnrichedSpanUtils.isEntrySpan(event);
    boolean isExitSpan = EnrichedSpanUtils.isExitSpan(event);

    // does not need to build the full traversal graph, just get the parents mapping
    StructuredTraceGraph graph = buildGraph(trace);

    if (isEntrySpan) {
      /*
      Determines if this is entry span is an entry to  api
      1. If the event is an ENTRY span, and
      2. If the direct parent span is either an EXIT Span or Null, then this is the entry point.
      */

      Event parentEvent = graph.getParentEvent(event);
      if (!EnrichedSpanUtils.isEntrySpan(parentEvent)) {
        addEnrichedAttribute(event, API_BOUNDARY_TYPE_ATTR_NAME,
            AttributeValueCreator.create(ENTRY_BOUNDARY_TYPE));

        // For all API entry spans, try to enrich with host header.
        enrichHostHeader(event);
      }
    } else if (isExitSpan) {
      /*
      Determines if this is exit span is an exit to  api
      1. If the event is an EXIT span, and
      2. If the direct child span is either an ENTRY Span or Null, then this is the exit point.
      */

      List<Event> childrenEvents = graph.getChildrenEvents(event);
      if (childrenEvents == null || childrenEvents.isEmpty()) {
        addEnrichedAttribute(event, API_BOUNDARY_TYPE_ATTR_NAME,
            AttributeValueCreator.create(EXIT_BOUNDARY_TYPE));
      } else {
        for (Event childEvent : childrenEvents) {
          if (EnrichedSpanUtils.isEntrySpan(childEvent)) {
            addEnrichedAttribute(event, API_BOUNDARY_TYPE_ATTR_NAME,
                AttributeValueCreator.create(EXIT_BOUNDARY_TYPE));
            break;
          }
        }
      }
    }
  }

  /**
   * Extracts the host header from the span and adds it as an enriched attributed to the span.
   * Note: This could potentially be either pulled into a separate enricher later or this
   * enricher class itself could be renamed to be more specific.
   */
  private void enrichHostHeader(Event event) {
    for (String key: HOST_HEADER_ATTRIBUTES) {
      String value = SpanAttributeUtils.getStringAttribute(event, key);
      if (value != null && !value.isEmpty()) {
        // Ignore if it's "localhost"
        String host = sanitizeHostValue(value);
        if (!LOCALHOST.equalsIgnoreCase(host)) {
          addEnrichedAttribute(event, HOST_HEADER, AttributeValueCreator.create(host));
          break;
        }
      }
    }
  }

  private String sanitizeHostValue(String host) {
    if (host.contains(COLON) && !host.startsWith(COLON)) {
      return COLON_SPLITTER.splitToList(host).get(0);
    }

    // the value is a URL, just return the authority part of it.
    try {
      URI uri = new URI(host);
      if (uri.getScheme() != null) {
        return uri.getHost();
      }
      return host;
    } catch (URISyntaxException ignore) {
      // ignore
      return host;
    }
  }
}
