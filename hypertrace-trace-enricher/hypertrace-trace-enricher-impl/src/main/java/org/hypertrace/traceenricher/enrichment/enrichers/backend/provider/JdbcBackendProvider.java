package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import static org.hypertrace.traceenricher.util.EnricherUtil.createAttributeValue;

import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.RateLimiter;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Sql;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcBackendProvider implements BackendProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBackendProvider.class);
  private static final String JDBC_EVENT_PREFIX = "jdbc";
  private static final RateLimiter INVALID_BACKEND_URL_LIMITER = RateLimiter.create(1 / 60d);
  private static final Splitter COLON_SPLITTER = Splitter.on(":");

  private Supplier<Optional<String>> sqlUriSupplier;
  private Supplier<Optional<String>> backendUriSupplier;

  @Override
  public void init(Event event) {
    this.sqlUriSupplier = Suppliers.memoize(() -> computeSqlUri(event))::get;
    this.backendUriSupplier = Suppliers.memoize(this::computeBackendUri)::get;
  }

  @Override
  public boolean isValidBackend(Event event) {
    return DbSemanticConventionUtils.isSqlBackend(event);
  }

  @Override
  public BackendType getBackendType(Event event) {
    return BackendType.JDBC;
  }

  @Override
  public Optional<String> getBackendUri(Event event, StructuredTraceGraph structuredTraceGraph) {
    Optional<String> maybeBackendUri = getBackendUri();
    if (maybeBackendUri.isEmpty()) {
      return Optional.empty();
    }

    String backendUri = maybeBackendUri.get();

    try {
      URI backendURI = URI.create(backendUri);
      if (backendURI.getHost() != null) {
        backendUri = String.format("%s:%d", backendURI.getHost(), backendURI.getPort());
      }
    } catch (Exception e) {
      if (INVALID_BACKEND_URL_LIMITER.tryAcquire()) {
        LOGGER.warn("Could not parse the jdbc URL: {}", backendUri, e);
      }
    }

    return Optional.ofNullable(backendUri);
  }

  @Override
  public Map<String, AttributeValue> getEntityAttributes(Event event) {
    Map<String, AttributeValue> entityAttributes = new HashMap<>();

    getDbType(event)
        .ifPresent(
            dbType ->
                entityAttributes.put(
                    RawSpanConstants.getValue(Sql.SQL_DB_TYPE), createAttributeValue(dbType)));
    getPath()
        .ifPresent(
            path ->
                entityAttributes.put(
                    EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH),
                    createAttributeValue(path)));

    return Collections.unmodifiableMap(entityAttributes);
  }

  @Override
  public Optional<String> getBackendOperation(Event event) {
    return DbSemanticConventionUtils.getDbOperationForJDBC(event);
  }

  @Override
  public Optional<String> getBackendDestination(Event event) {
    return DbSemanticConventionUtils.getDestinationForJdbc(event);
  }

  private Optional<String> getDbType(Event event) {
    // sql uri sample value: "jdbc:mysql://mysql:3306/shop"
    Optional<String> maybeSqlUri = getSqlUri();
    if (maybeSqlUri.isEmpty()) {
      return Optional.empty();
    }

    String sqlUri = maybeSqlUri.get();
    // Split the URL based on two slashes to extract the host and port values.
    String[] parts = sqlUri.split("//");
    String dbType = parts.length > 1 ? getDbType(parts[0]) : JDBC_EVENT_PREFIX;

    Optional<String> maybeDbType = DbSemanticConventionUtils.getDbTypeForOtelFormat(event);
    if (maybeDbType.isPresent() && !StringUtils.isEmpty(maybeDbType.get())) {
      dbType = maybeDbType.get();
    }

    return Optional.ofNullable(dbType);
  }

  private String getDbType(String scheme) {
    List<String> parts = COLON_SPLITTER.splitToList(scheme);
    // If there is more than one parts, returns the second one as the DB type. If not,
    // default to "jdbc", which is the first part.
    return parts.size() > 1 ? parts.get(1) : parts.get(0);
  }

  private Optional<String> getPath() {
    Optional<String> maybeBackendUri = getBackendUri();
    if (maybeBackendUri.isEmpty()) {
      return Optional.empty();
    }

    String backendUri = maybeBackendUri.get();

    try {
      URI backendURI = URI.create(backendUri);
      return Optional.of(backendURI.getPath());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private Optional<String> computeSqlUri(Event event) {
    Optional<String> maybeSqlUri = DbSemanticConventionUtils.getSqlURI(event);

    // sql uri sample value: "jdbc:mysql://mysql:3306/shop"
    if (maybeSqlUri.isEmpty() || StringUtils.isEmpty(maybeSqlUri.get())) {
      LOGGER.warn("Unable to infer a jdbc backend from event: {}", event);
      return Optional.empty();
    }

    return maybeSqlUri;
  }

  private Optional<String> computeBackendUri() {
    Optional<String> maybeSqlUri = getSqlUri();
    if (maybeSqlUri.isEmpty()) {
      return Optional.empty();
    }

    String sqlUri = maybeSqlUri.get();
    // Split the URL based on two slashes to extract the host and port values.
    String[] parts = sqlUri.split("//");
    if (parts.length > 1) {
      return Optional.of(JDBC_EVENT_PREFIX + "://" + parts[1]);
    }

    return Optional.of(sqlUri);
  }

  private Optional<String> getSqlUri() {
    return this.sqlUriSupplier.get();
  }

  private Optional<String> getBackendUri() {
    return this.backendUriSupplier.get();
  }
}
