package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.createAttributeValue;
import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeForFirstExistingKey;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.semantic.convention.utils.http.HttpSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpBackendResolver extends AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpBackendResolver.class);
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);

    if (protocol == Protocol.PROTOCOL_HTTP || protocol == Protocol.PROTOCOL_HTTPS) {
      Optional<Request> httpRequest =
          Optional.ofNullable(event.getHttp())
              .map(org.hypertrace.core.datamodel.eventfields.http.Http::getRequest);
      String backend = httpRequest.map(Request::getHost).orElse(null);
      String path = httpRequest.map(Request::getPath).orElse(null);

      if (StringUtils.isEmpty(backend)) {
        // Shouldn't reach here.
        LOGGER.warn("Unable to infer a http backend from event: {}", event);
        return Optional.empty();
      }

      BackendType type =
          (protocol == Protocol.PROTOCOL_HTTPS) ? BackendType.HTTPS : BackendType.HTTP;
      final Builder entityBuilder = getBackendEntityBuilder(type, backend, event);
      if (StringUtils.isNotEmpty(path)) {
        entityBuilder.putAttributes(
            EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH),
            createAttributeValue(path));
      }
      setAttributeForFirstExistingKey(
          event, entityBuilder, HttpSemanticConventionUtils.getAttributeKeysForHttpMethod());
      setAttributeForFirstExistingKey(
          event, entityBuilder, HttpSemanticConventionUtils.getAttributeKeysForHttpRequestMethod());

      Map<String, AttributeValue> enrichedAttributes = new HashMap<>();
      String httpMethodAttributeKey =
          SpanAttributeUtils.getFirstAvailableStringAttribute(
              event,
              ImmutableList.copyOf(
                  Iterables.concat(
                      HttpSemanticConventionUtils.getAttributeKeysForHttpMethod(),
                      HttpSemanticConventionUtils.getAttributeKeysForHttpMethod())));
      if (httpMethodAttributeKey != null) {
        AttributeValue operation =
            SpanAttributeUtils.getAttributeValue(event, httpMethodAttributeKey);
        enrichedAttributes.put(BACKEND_OPERATION_ATTR, operation);
      }
      return Optional.of(new BackendInfo(entityBuilder.build(), enrichedAttributes));
    }
    return Optional.empty();
  }
}
