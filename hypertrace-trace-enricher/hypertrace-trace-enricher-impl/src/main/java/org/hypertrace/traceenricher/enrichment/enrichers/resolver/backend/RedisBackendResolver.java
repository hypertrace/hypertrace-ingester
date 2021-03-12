package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.setAttributeIfExist;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.semantic.convention.constants.db.OTelDbSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Redis;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisBackendResolver extends AbstractBackendResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisBackendResolver.class);
  private static final String BACKEND_OPERATION_ATTR =
      EnrichedSpanConstants.getValue(Backend.BACKEND_OPERATION);

  @Override
  public Optional<BackendInfo> resolve(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (!DbSemanticConventionUtils.isRedisBackend(event)) {
      return Optional.empty();
    }

    Optional<String> backendURI = DbSemanticConventionUtils.getRedisURI(event);

    if (backendURI.isEmpty()) {
      return Optional.empty();
    }

    if (StringUtils.isEmpty(backendURI.get())) {
      LOGGER.warn("Unable to infer a redis backend from event: {}", event);
      return Optional.empty();
    }

    final Builder entityBuilder =
        getBackendEntityBuilder(BackendType.REDIS, backendURI.get(), event);
    setAttributeIfExist(event, entityBuilder, RawSpanConstants.getValue(Redis.REDIS_COMMAND));
    setAttributeIfExist(event, entityBuilder, RawSpanConstants.getValue(Redis.REDIS_ARGS));
    setAttributeIfExist(
        event, entityBuilder, OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue());

    Map<String, AttributeValue> enrichedAttributes = new HashMap<>();
    Optional<String> redisOperation = DbSemanticConventionUtils.getDbOperationForRedis(event);
    if (redisOperation != null) {
      enrichedAttributes.put(
          BACKEND_OPERATION_ATTR, AttributeValueCreator.create(redisOperation.get()));
    }
    return Optional.of(new BackendInfo(entityBuilder.build(), enrichedAttributes));
  }
}
