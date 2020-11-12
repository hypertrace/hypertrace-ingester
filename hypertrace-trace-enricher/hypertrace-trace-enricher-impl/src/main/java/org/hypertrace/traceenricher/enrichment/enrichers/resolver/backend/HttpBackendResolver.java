package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.createAttributeValue;
import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeIfExist;

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.http.Request;
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

  public HttpBackendResolver(FQNResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);

    if (protocol == Protocol.PROTOCOL_HTTP || protocol == Protocol.PROTOCOL_HTTPS) {
      Optional<Request> httpRequest = Optional.ofNullable(event.getHttp())
              .map(org.hypertrace.core.datamodel.eventfields.http.Http::getRequest);
      String backend = httpRequest.map(Request::getHost).orElse(null);
      String path = httpRequest.map(Request::getPath).orElse(null);

      if (StringUtils.isEmpty(backend)) {
        // Shouldn't reach here.
        LOGGER.warn("Unable to infer a http backend from event: {}", event);
        return Optional.empty();
      }

      BackendType type = (protocol == Protocol.PROTOCOL_HTTPS) ? BackendType.HTTPS : BackendType.HTTP;
      final Builder entityBuilder = getBackendEntityBuilder(type, backend, event);
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
