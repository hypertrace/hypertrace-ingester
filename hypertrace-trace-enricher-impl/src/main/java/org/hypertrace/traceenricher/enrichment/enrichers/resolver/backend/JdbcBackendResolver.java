package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.createAttributeValue;

import java.net.URI;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Sql;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcBackendResolver extends AbstractBackendResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBackendResolver.class);
  private static final String JDBC_EVENT_PREFIX = "jdbc";
  private static final String SQL_URL_ATTR = RawSpanConstants.getValue(Sql.SQL_SQL_URL);

  public JdbcBackendResolver(FQNResolver fqnResolver) {
    super(fqnResolver);
  }

  @Override
  public Optional<Entity> resolveEntity(Event event, StructuredTraceGraph structuredTraceGraph) {
    if (event.getEventName() != null && event.getEventName().startsWith(JDBC_EVENT_PREFIX) &&
        SpanAttributeUtils.containsAttributeKey(event, SQL_URL_ATTR)) {
      String backendUriStr = SpanAttributeUtils.getStringAttribute(event, SQL_URL_ATTR);

      // backendUriStr Sample value: "jdbc:mysql://mysql:3306/shop"
      if (StringUtils.isEmpty(backendUriStr)) {
        LOGGER.warn("Unable to infer a jdbc backend from event: {}", event);
        return Optional.empty();
      }

      final String revisedBackendUriStr = backendUriStr.replaceFirst("^jdbc:", "");
      URI backendURI = URI.create(revisedBackendUriStr);
      String path = backendURI.getPath();
      String dbScheme = backendURI.getScheme();
      backendUriStr = String.format("%s:%d", backendURI.getHost(), backendURI.getPort());
      final Builder entityBuilder = getBackendEntityBuilder(BackendType.JDBC, backendUriStr, event);
      if (StringUtils.isNotEmpty(path)) {
        entityBuilder.putAttributes(
            EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH), createAttributeValue(path));
      }
      if (StringUtils.isNotEmpty(dbScheme)) {
        entityBuilder
            .putIdentifyingAttributes(RawSpanConstants.getValue(Sql.SQL_DB_TYPE),
                createAttributeValue(dbScheme));
      }
      return Optional.of(entityBuilder.build());
    }
    return Optional.empty();
  }
}
