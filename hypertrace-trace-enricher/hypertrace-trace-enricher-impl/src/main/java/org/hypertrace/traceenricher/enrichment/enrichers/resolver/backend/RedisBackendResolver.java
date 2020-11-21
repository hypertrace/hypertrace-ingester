package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeIfExist;

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.attributeutils.db.DbAttributeUtils;
import org.hypertrace.attributeutils.db.OTelDbAttributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Redis;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisBackendResolver extends AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(RedisBackendResolver.class);

  public RedisBackendResolver(FQNResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (!DbAttributeUtils.isRedisBackend(event)) {
      return Optional.empty();
    }

    Optional<String> backendURI = DbAttributeUtils.getRedisURI(event);

    if (backendURI.isEmpty()) {
      return Optional.empty();
    }

    if (StringUtils.isEmpty(backendURI.get())) {
      LOGGER.warn("Unable to infer a redis backend from event: {}", event);
      return Optional.empty();
    }

    final Builder entityBuilder = getBackendEntityBuilder(BackendType.REDIS, backendURI.get(), event);
    setAttributeIfExist(event, entityBuilder, RawSpanConstants.getValue(Redis.REDIS_COMMAND));
    setAttributeIfExist(event, entityBuilder, RawSpanConstants.getValue(Redis.REDIS_ARGS));
    setAttributeIfExist(event, entityBuilder, OTelDbAttributes.DB_CONNECTION_STRING.getValue());
    return Optional.of(entityBuilder.build());
  }
}
