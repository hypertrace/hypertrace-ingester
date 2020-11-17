package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeIfExist;

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Redis;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;
import org.hypertrace.traceenricher.tagresolver.DbTagResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisBackendResolver extends AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(RedisBackendResolver.class);

  public RedisBackendResolver(FQNResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph) {
    Optional<String> backendURI = DbTagResolver.getRedisURI(event);
    if (backendURI.isEmpty()) {
      return Optional.empty();
    }

    if (StringUtils.isEmpty(backendURI.get())) {
      LOGGER.warn("Unable to infer a redis backend from event: {}", event);
      return Optional.empty();
    }

    final Builder entityBuilder = getBackendEntityBuilder(
        BackendType.REDIS, backendURI.get(), event);

    // todo: can't figure out otel equivalent, should we set db.connection_string for otel?
    setAttributeIfExist(event, entityBuilder, RawSpanConstants.getValue(Redis.REDIS_COMMAND));
    setAttributeIfExist(event, entityBuilder, RawSpanConstants.getValue(Redis.REDIS_ARGS));
    return Optional.of(entityBuilder.build());
  }
}
