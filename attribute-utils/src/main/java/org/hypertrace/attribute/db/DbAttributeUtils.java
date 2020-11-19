package org.hypertrace.attribute.db;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Mongo;
import org.hypertrace.core.span.constants.v1.Redis;
import org.hypertrace.core.span.constants.v1.Sql;

public class DbAttributeUtils {

  // db related OTEL attributes
  private static final String OTEL_DB_SYSTEM = OTelDbAttributes.DB_SYSTEM.getValue();
  private static final String OTEL_DB_CONNECTION_STRING = OTelDbAttributes.DB_CONNECTION_STRING.getValue();
  private static final String OTEL_DB_OPERATION = OTelDbAttributes.DB_OPERATION.getValue();
  private static final String OTEL_NET_PEER_IP = OTelDbAttributes.NET_PEER_IP.getValue();
  private static final String OTEL_NET_PEER_PORT = OTelDbAttributes.NET_PEER_PORT.getValue();
  private static final String OTEL_NET_PEER_NAME = OTelDbAttributes.NET_PEER_NAME.getValue();
  private static final String OTEL_NET_TRANSPORT = OTelDbAttributes.NET_TRANSPORT.getValue();

  // mongo specific attributes
  private static final String OTEL_MONGO_DB_SYSTEM_VALUE = OTelDbAttributes.MONGODB_SYSTEM_VALUE.getValue();
  private static final String OTHER_MONGO_ADDRESS = RawSpanConstants.getValue(Mongo.MONGO_ADDRESS);
  private static final String OTHER_MONGO_URL = RawSpanConstants.getValue(Mongo.MONGO_URL);
  private static final String OTHER_MONGO_OPERATION = RawSpanConstants.getValue(Mongo.MONGO_OPERATION);
  private static final String OTHER_MONGO_NAMESPACE = RawSpanConstants.getValue(Mongo.MONGO_NAMESPACE);
  private static final String OTEL_MONGO_COLLECTION = OTelDbAttributes.MONGODB_COLLECTION.getValue();
  //private static final AttributeValue MONGO_DEFAULT_PORT = EnricherUtil.createAttributeValue(EnricherUtil.createAttributeValue("27017"));

  // redis specific attributes
  private static final String OTHER_REDIS_CONNECTION = RawSpanConstants.getValue(Redis.REDIS_CONNECTION);
  private static final String OTEL_REDIS_DB_SYSTEM_VALUE = "redis";
  //private static final AttributeValue REDIS_DEFAULT_PORT = EnricherUtil.createAttributeValue(EnricherUtil.createAttributeValue("6379"));

  // sql specific attributes
  private static final String[] OTEL_SQL_DB_SYSTEM_VALUES =
      {
          "mysql", "oracle", "mssql",
          "other_sql", "db2", "postgresql",
          "redshift", "hive", "cloudscape", "hsqldb"
      };
  private static final String JDBC_EVENT_PREFIX = "jdbc";
  private static final String SQL_URL = RawSpanConstants.getValue(Sql.SQL_SQL_URL);

  public static Optional<String> getMongoURI(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTHER_MONGO_ADDRESS)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTHER_MONGO_ADDRESS));
    } else if (SpanAttributeUtils.containsAttributeKey(event, OTHER_MONGO_URL)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTHER_MONGO_URL));
    } else if (SpanAttributeUtils.containsAttributeKey(event, OTEL_DB_SYSTEM)) {
      if (!OTEL_MONGO_DB_SYSTEM_VALUE.equals(event.getAttributes().getAttributeMap().get(
          OTEL_DB_SYSTEM).getValue())) {
        return Optional.empty();
      }
      return getSqlURIForOtelFormat(event);
    }

    return Optional.empty();
  }

  public static List<String> getTagsForMongoOperation() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_MONGO_OPERATION, OTEL_DB_OPERATION));
  }

  public static List<String> getTagsForMongoNamespace() {
    return Lists.newArrayList(Sets.newHashSet(OTEL_MONGO_COLLECTION, OTHER_MONGO_NAMESPACE));
  }

  public static Optional<String> getRedisURI(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTHER_REDIS_CONNECTION)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTHER_REDIS_CONNECTION));
    }
    if (event.getAttributes().getAttributeMap().containsKey(OTEL_DB_SYSTEM)) {
      if (!OTEL_REDIS_DB_SYSTEM_VALUE.equals(
          event.getAttributes().getAttributeMap().get(OTEL_DB_SYSTEM).getValue())) {
        return Optional.empty();
      }
      return getSqlURIForOtelFormat(event);
    }
    return Optional.empty();
  }

  public static boolean isSqlBackend(Event event) {
    if (event.getEventName() != null
        && event.getEventName().startsWith(JDBC_EVENT_PREFIX)
        && SpanAttributeUtils.containsAttributeKey(event, SQL_URL)) {
      return true;
    }
    return isSqlTypeBackendForOtelFormat(event);
  }

  public static boolean isSqlTypeBackendForOtelFormat(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTEL_DB_SYSTEM)) {
      return Arrays
          .stream(OTEL_SQL_DB_SYSTEM_VALUES)
          .anyMatch(
              v -> v.equals(
                  event.getAttributes().getAttributeMap().get(OTEL_DB_SYSTEM).getValue()));
    }
    return false;
  }

  public static boolean isSqlTypeBackendForOtelFormat(Map<String, AttributeValue> attributeMap) {
    if (attributeMap.containsKey(OTEL_DB_SYSTEM)) {
      return Arrays
          .stream(OTEL_SQL_DB_SYSTEM_VALUES)
          .anyMatch(
              v -> v.equals(
                  attributeMap.get(OTEL_DB_SYSTEM).getValue()));
    }
    return false;
  }

  public static Optional<String> getSqlURI(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, SQL_URL)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, SQL_URL));
    }
    return getSqlURIForOtelFormat(event);
  }

  public static Optional<String> getSqlURIForOtelFormat(Event event) {
    return getSqlURIForOtelFormat(event.getAttributes().getAttributeMap());
  }

  public static Optional<String> getSqlURIForOtelFormat(
      Map<String, AttributeValue> attributeMap) {
    if ((attributeMap.containsKey(OTEL_NET_PEER_NAME)
        || attributeMap.containsKey(OTEL_NET_PEER_IP))
        && attributeMap.containsKey(OTEL_NET_PEER_PORT)) {
      String host = attributeMap.getOrDefault(
          OTEL_NET_PEER_NAME,
          attributeMap.get(OTEL_NET_PEER_IP)).getValue();
      String port = attributeMap.get(OTEL_NET_PEER_PORT).getValue();
      return Optional.of(String.format("%s:%s", host, port));
    }
    return Optional.empty();
  }

  public static Optional<String> getDbTypeForOtelFormat(Event event) {
    return getDbTypeForOtelFormat(event.getAttributes().getAttributeMap());
  }

  public static Optional<String> getDbTypeForOtelFormat(Map<String, AttributeValue> attributeMap) {
    if (attributeMap.containsKey(OTEL_DB_SYSTEM)) {
      return Optional.of(attributeMap.get(OTEL_DB_SYSTEM).getValue());
    }
    return Optional.empty();
  }
}
