package org.hypertrace.telemetry.attribute.utils.db;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Mongo;
import org.hypertrace.core.span.constants.v1.Redis;
import org.hypertrace.core.span.constants.v1.Sql;

/**
 * Utility class to fetch database span attributes
 */
public class DbSemanticConventionUtils {

  // db related OTEL attributes
  private static final String OTEL_DB_SYSTEM = OTelDbSemanticConventions.DB_SYSTEM.getValue();
  private static final String OTEL_DB_CONNECTION_STRING = OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue();
  private static final String OTEL_DB_OPERATION = OTelDbSemanticConventions.DB_OPERATION.getValue();
  private static final String OTEL_NET_PEER_IP = OTelDbSemanticConventions.NET_PEER_IP.getValue();
  private static final String OTEL_NET_PEER_PORT = OTelDbSemanticConventions.NET_PEER_PORT.getValue();
  private static final String OTEL_NET_PEER_NAME = OTelDbSemanticConventions.NET_PEER_NAME.getValue();
  private static final String OTEL_NET_TRANSPORT = OTelDbSemanticConventions.NET_TRANSPORT.getValue();

  // mongo specific attributes
  private static final String OTEL_MONGO_DB_SYSTEM_VALUE = OTelDbSemanticConventions.MONGODB_DB_SYSTEM_VALUE.getValue();
  private static final String OTHER_MONGO_ADDRESS = RawSpanConstants.getValue(Mongo.MONGO_ADDRESS);
  private static final String OTHER_MONGO_URL = RawSpanConstants.getValue(Mongo.MONGO_URL);
  private static final String OTHER_MONGO_OPERATION = RawSpanConstants.getValue(Mongo.MONGO_OPERATION);
  private static final String OTHER_MONGO_NAMESPACE = RawSpanConstants.getValue(Mongo.MONGO_NAMESPACE);
  private static final String OTEL_MONGO_COLLECTION = OTelDbSemanticConventions.MONGODB_COLLECTION.getValue();

  // redis specific attributes
  private static final String OTHER_REDIS_CONNECTION = RawSpanConstants.getValue(Redis.REDIS_CONNECTION);
  private static final String OTEL_REDIS_DB_SYSTEM_VALUE = OTelDbSemanticConventions.REDIS_DB_SYSTEM_VALUE.getValue();

  // sql specific attributes
  private static final String[] OTEL_SQL_DB_SYSTEM_VALUES =
      {
          OTelDbSemanticConventions.MYSQL_DB_SYSTEM_VALUE.getValue(), OTelDbSemanticConventions.ORACLE_DB_SYSTEM_VALUE.getValue(),
          OTelDbSemanticConventions.MSSQL_DB_SYSTEM_VALUE.getValue(), OTelDbSemanticConventions.DB2_DB_SYSTEM_VALUE.getValue(),
          OTelDbSemanticConventions.POSTGRESQL_DB_SYSTEM_VALUE.getValue(), OTelDbSemanticConventions.REDSHIFT_DB_SYSTEM_VALUE.getValue(),
          OTelDbSemanticConventions.HIVE_DB_SYSTEM_VALUE.getValue(), OTelDbSemanticConventions.CLOUDSCAPE_DB_SYSTEM_VALUE.getValue(),
          OTelDbSemanticConventions.HSQLDB_DB_SYSTEM_VALUE.getValue(), OTelDbSemanticConventions.OTHER_SQL_DB_SYSTEM_VALUE.getValue()
      };
  private static final String JDBC_EVENT_PREFIX = "jdbc";
  private static final String SQL_URL = RawSpanConstants.getValue(Sql.SQL_SQL_URL);

  /**
   * @param event Object encapsulating span data
   * @return check if this span is for a mongo backend
   */
  public static boolean isMongoBackend(Event event) {
    return SpanAttributeUtils.containsAttributeKey(event, OTHER_MONGO_ADDRESS)
        || SpanAttributeUtils.containsAttributeKey(event, OTHER_MONGO_URL)
        || OTEL_MONGO_DB_SYSTEM_VALUE.equals(SpanAttributeUtils.getStringAttributeWithDefault(
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

  /**
   * @return attribute keys representing mongo operation
   */
  public static List<String> getAttributeKeysForMongoOperation() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_MONGO_OPERATION, OTEL_DB_OPERATION));
  }

  /**
   * @return attribute keys representing mongo namespace
   */
  public static List<String> getAttributeKeysForMongoNamespace() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_MONGO_NAMESPACE, OTEL_MONGO_COLLECTION));
  }

  /**
   * @param event Object encapsulating span data
   * @return check if this span is for a redis backend
   */
  public static boolean isRedisBackend(Event event) {
    return SpanAttributeUtils.containsAttributeKey(event, OTHER_REDIS_CONNECTION)
        || OTEL_REDIS_DB_SYSTEM_VALUE.equals(SpanAttributeUtils.getStringAttributeWithDefault(
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
  static boolean isSqlBackend(Event event) {
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
      return Arrays
          .stream(OTEL_SQL_DB_SYSTEM_VALUES)
          .anyMatch(v -> v.equals(
              SpanAttributeUtils.getStringAttribute(event, OTEL_DB_SYSTEM)));
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
    return getBackendURIForOtelFormat(event);
  }

  /**
   * @param event Object encapsulating span data
   * @return backend uri based on otel format
   */
  public static Optional<String> getBackendURIForOtelFormat(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTEL_DB_CONNECTION_STRING)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTEL_DB_CONNECTION_STRING));
    } else {
      String host = SpanAttributeUtils.getStringAttributeWithDefault(
          event, OTEL_NET_PEER_NAME,
          SpanAttributeUtils.getStringAttribute(event, OTEL_NET_PEER_IP));
      if (StringUtils.isBlank(host)) {
        return Optional.empty();
      }
      if (SpanAttributeUtils.containsAttributeKey(event, OTEL_NET_PEER_PORT)) {
        return Optional.of(String.format(
            "%s:%s", host, SpanAttributeUtils.getStringAttribute(event, OTEL_NET_PEER_PORT)));
      }
      return Optional.of(host);
    }
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
}
