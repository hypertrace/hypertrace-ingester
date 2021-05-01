package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.getAttributesForFirstExistingKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.semantic.convention.constants.db.OTelDbSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Redis;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;

public class RedisBackendResolver extends AbstractBackendResolver {
  public RedisBackendResolver(FqnResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public boolean isValidBackend(Event event) {
    return DbSemanticConventionUtils.isRedisBackend(event);
  }

  @Override
  public BackendType getBackendType(Event event) {
    return BackendType.REDIS;
  }

  @Override
  public Optional<String> getBackendUri(Event event, StructuredTraceGraph structuredTraceGraph) {
    return DbSemanticConventionUtils.getRedisURI(event);
  }

  @Override
  public Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> getEntityAttributes(
      Event event) {
    Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> entityAttributes =
        new HashMap<>();
    entityAttributes.putAll(
        getAttributesForFirstExistingKey(
            event, List.of(RawSpanConstants.getValue(Redis.REDIS_COMMAND))));
    entityAttributes.putAll(
        getAttributesForFirstExistingKey(
            event, List.of(RawSpanConstants.getValue(Redis.REDIS_ARGS))));
    entityAttributes.putAll(
        getAttributesForFirstExistingKey(
            event, List.of(OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue())));
    return Collections.unmodifiableMap(entityAttributes);
  }

  @Override
  public Optional<String> getBackendOperation(Event event) {
    return DbSemanticConventionUtils.getDbOperationForRedis(event);
  }

  @Override
  public Optional<String> getBackendDestination(Event event) {
    return DbSemanticConventionUtils.getDestinationForRedis(event);
  }
}
