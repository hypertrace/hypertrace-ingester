package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeIfExist;

import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Mongo;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoBackendResolver extends AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoBackendResolver.class);
  private static final String RAW_MONGO_NAMESPACE = "NAMESPACE";
  private static final String MONGO_ADDRESS_ATTR = RawSpanConstants.getValue(Mongo.MONGO_ADDRESS);
  private static final String MONGO_URL_ATTR = RawSpanConstants.getValue(Mongo.MONGO_URL);

  public MongoBackendResolver(FQNResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (SpanAttributeUtils.containsAttributeKey(event, MONGO_ADDRESS_ATTR) ||
        SpanAttributeUtils.containsAttributeKey(event, MONGO_URL_ATTR)) {
      String backendURI = null;
      // check if this is from uninstrumeted agent first
      if (SpanAttributeUtils.containsAttributeKey(event, MONGO_ADDRESS_ATTR)) {
        backendURI = SpanAttributeUtils.getStringAttribute(event, MONGO_ADDRESS_ATTR);
      } else if (SpanAttributeUtils.containsAttributeKey(event, MONGO_URL_ATTR)) {
        backendURI = SpanAttributeUtils.getStringAttribute(event, MONGO_URL_ATTR);
      }

      if (backendURI == null) {
        LOGGER.debug("Detected MONGO span event, but unable to extract the URI. Span Event: {}",
            event);
        return Optional.empty();
      }

      final Builder entityBuilder = getBackendEntityBuilder(BackendType.MONGO, backendURI, event);
      setAttributeIfExist(event, entityBuilder, RAW_MONGO_NAMESPACE);
      setAttributeIfExist(event, entityBuilder, RawSpanConstants.getValue(Mongo.MONGO_NAMESPACE));
      setAttributeIfExist(event, entityBuilder, RawSpanConstants.getValue(Mongo.MONGO_OPERATION));
      return Optional.of(entityBuilder.build());
    }
    return Optional.empty();
  }
}
