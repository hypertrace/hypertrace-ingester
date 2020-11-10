package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.hypertrace.traceenricher.util.EnricherUtil.createAttributeValue;

import com.google.common.base.Splitter;
import com.google.common.util.concurrent.RateLimiter;
import java.net.URI;
import java.util.List;
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
  private static final RateLimiter INVALID_BACKEND_URL_LIMITER = RateLimiter.create(1 / 60d);
  private static final Splitter COLON_SPLITTER = Splitter.on(":");

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

      // Split the URL based on two slashes to extract the host and port values.
      String[] parts = backendUriStr.split("//");
      String dbType;
      if (parts.length > 1) {
        backendUriStr = JDBC_EVENT_PREFIX + "://" + parts[1];
        dbType = getDbType(parts[0]);
      } else {
        dbType = JDBC_EVENT_PREFIX;
      }

      String path = null;
      try {
        URI backendURI = URI.create(backendUriStr);
        path = backendURI.getPath();
        if (backendURI.getHost() != null) {
          backendUriStr = String.format("%s:%d", backendURI.getHost(), backendURI.getPort());
        }
      } catch (Exception e) {
        if (INVALID_BACKEND_URL_LIMITER.tryAcquire()) {
          LOGGER.warn("Could not parse the jdbc URL: {}", backendUriStr, e);
        }
      }

      final Builder entityBuilder = getBackendEntityBuilder(BackendType.JDBC, backendUriStr, event);
      if (StringUtils.isNotEmpty(path)) {
        entityBuilder.putAttributes(
            EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH), createAttributeValue(path));
      }
      entityBuilder.putIdentifyingAttributes(RawSpanConstants.getValue(Sql.SQL_DB_TYPE),
          createAttributeValue(dbType));
      return Optional.of(entityBuilder.build());
    }
    return Optional.empty();
  }

  private String getDbType(String scheme) {
    List<String> parts = COLON_SPLITTER.splitToList(scheme);
    // If there is more than one parts, returns the second one as the DB type. If not,
    // default to "jdbc", which is the first part.
    return parts.size() > 1 ? parts.get(1) : parts.get(0);
  }
}
