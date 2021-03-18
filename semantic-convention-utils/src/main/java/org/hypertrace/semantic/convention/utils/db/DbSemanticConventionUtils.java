package org.hypertrace.semantic.convention.utils.db;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.semantic.convention.constants.db.OTelDbSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Mongo;
import org.hypertrace.core.span.constants.v1.Redis;
import org.hypertrace.core.span.constants.v1.Sql;
import org.hypertrace.semantic.convention.utils.span.SpanSemanticConventionUtils;

/** Utility class to fetch database span attributes */
public class DbSemanticConventionUtils {

  // db related OTEL attributes
  private static final String OTEL_DB_SYSTEM = OTelDbSemanticConventions.DB_SYSTEM.getValue();
  private static final String OTEL_DB_CONNECTION_STRING =
      OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue();
  private static final String OTEL_DB_OPERATION = OTelDbSemanticConventions.DB_OPERATION.getValue();
  private static final String OTEL_DB_STATEMENT = OTelDbSemanticConventions.DB_STATEMENT.getValue();
  private static final String OTEL_DB_NAME = OTelDbSemanticConventions.DB_NAME.getValue();

  // mongo specific attributes
  private static final String OTEL_MONGO_DB_SYSTEM_VALUE =
      OTelDbSemanticConventions.MONGODB_DB_SYSTEM_VALUE.getValue();
  private static final String OTHER_MONGO_ADDRESS = RawSpanConstants.getValue(Mongo.MONGO_ADDRESS);
  private static final String OTHER_MONGO_URL = RawSpanConstants.getValue(Mongo.MONGO_URL);
  private static final String OTHER_MONGO_OPERATION =
      RawSpanConstants.getValue(Mongo.MONGO_OPERATION);
  private static final String OTHER_MONGO_NAMESPACE =
      RawSpanConstants.getValue(Mongo.MONGO_NAMESPACE);
  private static final String OTEL_MONGO_COLLECTION =
      OTelDbSemanticConventions.MONGODB_COLLECTION.getValue();

  // redis specific attributes
  private static final String OTHER_REDIS_CONNECTION =
      RawSpanConstants.getValue(Redis.REDIS_CONNECTION);
  private static final String OTEL_REDIS_DB_SYSTEM_VALUE =
      OTelDbSemanticConventions.REDIS_DB_SYSTEM_VALUE.getValue();
  private static final String OTEL_REDIS_DB_INDEX =
      OTelDbSemanticConventions.REDIS_DB_INDEX.getValue();

  // sql specific attributes
  private static final String[] OTEL_SQL_DB_SYSTEM_VALUES = {
    OTelDbSemanticConventions.MYSQL_DB_SYSTEM_VALUE.getValue(),
    OTelDbSemanticConventions.ORACLE_DB_SYSTEM_VALUE.getValue(),
    OTelDbSemanticConventions.MSSQL_DB_SYSTEM_VALUE.getValue(),
    OTelDbSemanticConventions.DB2_DB_SYSTEM_VALUE.getValue(),
    OTelDbSemanticConventions.POSTGRESQL_DB_SYSTEM_VALUE.getValue(),
    OTelDbSemanticConventions.REDSHIFT_DB_SYSTEM_VALUE.getValue(),
    OTelDbSemanticConventions.HIVE_DB_SYSTEM_VALUE.getValue(),
    OTelDbSemanticConventions.CLOUDSCAPE_DB_SYSTEM_VALUE.getValue(),
    OTelDbSemanticConventions.HSQLDB_DB_SYSTEM_VALUE.getValue(),
    OTelDbSemanticConventions.OTHER_SQL_DB_SYSTEM_VALUE.getValue()
  };
  private static final String JDBC_EVENT_PREFIX = "jdbc";
  private static final String SQL_URL = RawSpanConstants.getValue(Sql.SQL_SQL_URL);
  private static final String SQL_TABLE_NAME = OTelDbSemanticConventions.SQL_TABLE_NAME.getValue();

  /**
   * @param event Object encapsulating span data
   * @return check if this span is for a mongo backend
   */
  public static boolean isMongoBackend(Event event) {
    return SpanAttributeUtils.containsAttributeKey(event, OTHER_MONGO_ADDRESS)
        || SpanAttributeUtils.containsAttributeKey(event, OTHER_MONGO_URL)
        || OTEL_MONGO_DB_SYSTEM_VALUE.equals(
            SpanAttributeUtils.getStringAttributeWithDefault(
                event, OTEL_DB_SYSTEM, StringUtils.EMPTY));
  }

  /**
   * @param event Object encapsulating span data
   * @return URI for mongo database
   */
  public static Optional<String> getMongoURI(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTHER_MONGO_ADDRESS)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTHER_MONGO_ADDRESS));
    } else if (SpanAttributeUtils.containsAttributeKey(event, OTHER_MONGO_URL)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTHER_MONGO_URL));
    }
    return getBackendURIForOtelFormat(event);
  }

  /** @return attribute keys representing mongo operation */
  public static List<String> getAttributeKeysForMongoOperation() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_MONGO_OPERATION, OTEL_DB_OPERATION));
  }

  /** @return attribute keys representing mongo namespace */
  public static List<String> getAttributeKeysForMongoNamespace() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_MONGO_NAMESPACE, OTEL_MONGO_COLLECTION));
  }

  public static List<String> getAttributeKeysForDbOperation() {
    return Lists.newArrayList(Sets.newHashSet(OTEL_DB_OPERATION));
  }

  public static List<String> getAttributeKeysForDbStatement() {
    return Lists.newArrayList(Sets.newHashSet(OTEL_DB_STATEMENT));
  }

  public static List<String> getAttributeKeysForDbName() {
    return Lists.newArrayList(Sets.newHashSet(OTEL_DB_NAME));
  }

  public static List<String> getAttributeKeysForSqlTableName() {
    return Lists.newArrayList(Sets.newHashSet(SQL_TABLE_NAME));
  }

  public static List<String> getAttributeKeysForRedisTableIndex() {
    return Lists.newArrayList(Sets.newHashSet(OTEL_REDIS_DB_INDEX));
  }

  public static Optional<String> getDbOperationForJDBC(Event event) {
    Optional<String> jdbcOperation = getOtelDbOperation(event);
    if (jdbcOperation.isPresent()) {
      return jdbcOperation;
    }
    String sqlQuery =
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, List.of(RawSpanConstants.getValue(Sql.SQL_QUERY)));
    if (sqlQuery != null) {
      return Optional.of(getOperationFromDbQuery(sqlQuery));
    }
    return Optional.empty();
  }

  public static Optional<String> getDbOperationForRedis(Event event) {
    Optional<String> redisOperation = getOtelDbOperation(event);
    if (redisOperation.isPresent()) {
      return redisOperation;
    }
    String redisCommand =
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, List.of(RawSpanConstants.getValue(Redis.REDIS_COMMAND)));
    if (redisCommand != null) {
      return Optional.of(redisCommand);
    }
    return Optional.empty();
  }

  public static Optional<String> getDbOperationForMongo(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, DbSemanticConventionUtils.getAttributeKeysForMongoOperation()));
  }

  public static Optional<String> getDestinationForJdbc(Event event) {
    Optional<String> sqlTableName = getSqlTableName(event);
    return getDestinationForDb(event, sqlTableName);
  }

  public static Optional<String> getDestinationForMongo(Event event) {
    Optional<String> mongoCollection = getMongoCollectionName(event);
    return getDestinationForDb(event, mongoCollection);
  }

  public static Optional<String> getDestinationForRedis(Event event) {
    Optional<String> redisDbIndex = getRedisDbIndex(event);
    return getDestinationForDb(event, redisDbIndex);
  }

  /**
   * @param event Object encapsulating span data
   * @return check if this span is for a redis backend
   */
  public static boolean isRedisBackend(Event event) {
    return SpanAttributeUtils.containsAttributeKey(event, OTHER_REDIS_CONNECTION)
        || OTEL_REDIS_DB_SYSTEM_VALUE.equals(
            SpanAttributeUtils.getStringAttributeWithDefault(
                event, OTEL_DB_SYSTEM, StringUtils.EMPTY));
  }

  /**
   * @param event Object encapsulating span data
   * @return URI for redis database
   */
  public static Optional<String> getRedisURI(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTHER_REDIS_CONNECTION)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTHER_REDIS_CONNECTION));
    }
    return getBackendURIForOtelFormat(event);
  }

  /**
   * @param event Object encapsulating span data
   * @return check if the event is for a sql backend
   */
  public static boolean isSqlBackend(Event event) {
    if (event.getEventName() != null
        && event.getEventName().startsWith(JDBC_EVENT_PREFIX)
        && SpanAttributeUtils.containsAttributeKey(event, SQL_URL)) {
      return true;
    }
    return isSqlTypeBackendForOtelFormat(event);
  }

  /**
   * @param event Object encapsulating span data
   * @return check if the event is for a sql backend based on otel format
   */
  public static boolean isSqlTypeBackendForOtelFormat(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTEL_DB_SYSTEM)) {
      return Arrays.stream(OTEL_SQL_DB_SYSTEM_VALUES)
          .anyMatch(v -> v.equals(SpanAttributeUtils.getStringAttribute(event, OTEL_DB_SYSTEM)));
    }
    return false;
  }

  /**
   * @param attributeValueMap attribute key value
   * @return check if the corresponding event is for a sql backend based on otel format
   */
  public static boolean isSqlTypeBackendForOtelFormat(
      Map<String, AttributeValue> attributeValueMap) {
    if (attributeValueMap.containsKey(OTEL_DB_SYSTEM)) {
      return Arrays.stream(OTEL_SQL_DB_SYSTEM_VALUES)
          .anyMatch(v -> v.equals(attributeValueMap.get(OTEL_DB_SYSTEM).getValue()));
    }
    return false;
  }

  /**
   * @param event Object encapsulating span data
   * @return sql uri for the event
   */
  public static Optional<String> getSqlURI(Event event) {
    if (!isSqlBackend(event)) {
      return Optional.empty();
    }
    if (SpanAttributeUtils.containsAttributeKey(event, SQL_URL)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, SQL_URL));
    }
    Optional<String> backendUrl = getBackendURIForOtelFormat(event);
    if (backendUrl.isPresent()) {
      return backendUrl;
    }
    if (SpanAttributeUtils.containsAttributeKey(
        event, OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue())) {
      String url =
          SpanAttributeUtils.getStringAttribute(
              event, OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue());
      if (!isValidURI(url)) {
        return Optional.empty();
      }
      return Optional.of(url);
    }
    return Optional.empty();
  }

  public static Optional<String> getSqlUrlForOtelFormat(
      Map<String, AttributeValue> attributeValueMap) {
    if (!isSqlTypeBackendForOtelFormat(attributeValueMap)) {
      return Optional.empty();
    }
    Optional<String> backendUrl = getBackendURIForOtelFormat(attributeValueMap);
    if (backendUrl.isPresent()) {
      return backendUrl;
    }
    if (attributeValueMap.containsKey(OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue())) {
      String url =
          attributeValueMap
              .get(OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue())
              .getValue();
      if (!isValidURI(url)) {
        return Optional.empty();
      }
      return Optional.of(url);
    }
    return Optional.empty();
  }

  /**
   * @param event Object encapsulating span data
   * @return backend uri based on otel format
   */
  public static Optional<String> getBackendURIForOtelFormat(Event event) {
    return SpanSemanticConventionUtils.getURIForOtelFormat(event);
  }

  public static Optional<String> getBackendURIForOpenTracingFormat(Event event) {
    return SpanSemanticConventionUtils.getURIforOpenTracingFormat(event);
  }

  /**
   * @param attributeValueMap map of attribute key value
   * @return backend uri based on otel format
   */
  public static Optional<String> getBackendURIForOtelFormat(
      Map<String, AttributeValue> attributeValueMap) {
    return SpanSemanticConventionUtils.getURIForOtelFormat(attributeValueMap);
  }

  /**
   * @param event Object encapsulating span data
   * @return database type (mysql, mongo ...) based on otel format
   */
  public static Optional<String> getDbTypeForOtelFormat(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTEL_DB_SYSTEM)) {
      return Optional.ofNullable(SpanAttributeUtils.getStringAttribute(event, OTEL_DB_SYSTEM));
    }
    return Optional.empty();
  }

  static boolean isValidURI(String uri) {
    try {
      new URI(uri);
    } catch (URISyntaxException e) {
      return false;
    }
    return true;
  }

  private static String getOperationFromDbQuery(String query) {
    return StringUtils.substringBefore(StringUtils.trim(query), " ");
  }

  private static Optional<String> getSqlTableName(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, getAttributeKeysForSqlTableName()));
  }

  private static Optional<String> getMongoCollectionName(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, getAttributeKeysForMongoNamespace()));
  }

  private static Optional<String> getRedisDbIndex(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, getAttributeKeysForRedisTableIndex()));
  }

  private static Optional<String> getOtelDbOperation(Event event) {
    String dbOperation =
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, DbSemanticConventionUtils.getAttributeKeysForDbOperation());
    if (dbOperation != null) {
      return Optional.of(dbOperation);
    }
    String dbStatement =
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, DbSemanticConventionUtils.getAttributeKeysForDbStatement());
    if (dbStatement != null) {
      return Optional.of(getOperationFromDbQuery(dbStatement));
    }
    return Optional.empty();
  }

  private static Optional<String> getDbName(Event event) {
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(event, getAttributeKeysForDbName()));
  }

  private static Optional<String> getDestinationForDb(Event event, Optional<String> tableName) {
    Optional<String> dbName = getDbName(event);
    if (dbName.isPresent() && tableName.isPresent()) {
      return Optional.of(
          (new StringBuilder()
              .append(dbName.get())
              .append(".")
              .append(tableName.get())
              .toString()));
    } else if (dbName.isPresent()) {
      return dbName;
    } else if (tableName.isPresent()) {
      return tableName;
    }
    return Optional.empty();
  }
}
