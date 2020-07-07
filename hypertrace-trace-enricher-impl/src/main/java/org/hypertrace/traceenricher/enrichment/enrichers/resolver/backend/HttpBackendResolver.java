package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.createAttributeValue;
import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeIfExist;

import java.net.URI;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpBackendResolver extends AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpBackendResolver.class);

  private static final String HTTP_URL_ATTR = RawSpanConstants.getValue(Http.HTTP_URL_WITH_HTTP);
  private static final String HTTP_HOST_ATTR = RawSpanConstants.getValue(Http.HTTP_HOST);
  private static final String HTTP_PATH_ATTR = RawSpanConstants.getValue(Http.HTTP_PATH);
  private static final String HTTP_REQUEST_URL_ATTR =
      RawSpanConstants.getValue(Http.HTTP_REQUEST_URL);

  public HttpBackendResolver(FQNResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);

    if (protocol == Protocol.PROTOCOL_HTTP) {
      String backendUriStr = null;
      String path = null;
      if (SpanAttributeUtils.containsAttributeKey(event, HTTP_URL_ATTR)) {
        backendUriStr = SpanAttributeUtils.getStringAttribute(event, HTTP_URL_ATTR);
      } else if (SpanAttributeUtils.containsAttributeKey(event, HTTP_REQUEST_URL_ATTR)) {
        backendUriStr = SpanAttributeUtils.getStringAttribute(event, HTTP_REQUEST_URL_ATTR);
      }

      if (StringUtils.isNotEmpty(backendUriStr)) {
        URI backendURI;
        try {
          backendURI = URI.create(backendUriStr);
          backendUriStr = String.format("%s:%d", backendURI.getHost(), backendURI.getPort());
          path = backendURI.getPath();
        } catch (IllegalArgumentException e) {
          LOGGER.warn("Failed to parse URL string {} to create URI", backendUriStr);
          backendUriStr = null;
        }
      }
      // if URL string was null or unable to parse URL string, check host attribute
      if (StringUtils.isEmpty(backendUriStr) && SpanAttributeUtils.containsAttributeKey(event, HTTP_HOST_ATTR)) {
        backendUriStr = SpanAttributeUtils.getStringAttribute(event, HTTP_HOST_ATTR);
        if (SpanAttributeUtils.containsAttributeKey(event, HTTP_PATH_ATTR)) {
          path = SpanAttributeUtils.getStringAttribute(event, HTTP_PATH_ATTR);
        }
      }

      if (StringUtils.isEmpty(backendUriStr)) {
        // Shouldn't reach here.
        LOGGER.warn("Unable to infer a http backend from event: {}", event);
        return Optional.empty();
      }
      final Builder entityBuilder = getBackendEntityBuilder(BackendType.HTTP, backendUriStr, event);
      if (StringUtils.isNotEmpty(path)) {
        entityBuilder.putAttributes(
            EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH),
            createAttributeValue(path));
      }
      setAttributeIfExist(event, entityBuilder, RawSpanConstants.getValue(Http.HTTP_METHOD));
      setAttributeIfExist(event, entityBuilder, RawSpanConstants.getValue(Http.HTTP_REQUEST_METHOD));
      return Optional.of(entityBuilder.build());
    }
    return Optional.empty();
  }
}
