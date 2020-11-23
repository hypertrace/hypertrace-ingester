package org.hypertrace.telemetry.attribute.utils.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.telemetry.attribute.utils.SemanticConventionTestUtil;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Mongo;
import org.hypertrace.core.span.constants.v1.Redis;
import org.hypertrace.core.span.constants.v1.Sql;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link DbSemanticConventionUtils}
 */
public class DbSemanticConventionUtilsTest {

  @Test
  public void isMongoBackend() {
    Event e = mock(Event.class);
    // otel format
    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue()),
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = DbSemanticConventionUtils.isMongoBackend(e);
    assertTrue(v);

    // otel format, dbsystem is not mongo
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(OTelDbAttributes.MYSQL_DB_SYSTEM_VALUE.getValue()),
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isMongoBackend(e);
    assertFalse(v);

    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Mongo.MONGO_ADDRESS),
            SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isMongoBackend(e);
    assertTrue(v);

    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Mongo.MONGO_URL),
            SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isMongoBackend(e);
    assertTrue(v);

    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            "span.kind",
            SemanticConventionTestUtil.buildAttributeValue("client")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isMongoBackend(e);
    assertFalse(v);
  }


  @Test
  public void testGetMongoURI() {
    Event e = mock(Event.class);
    // otel format
    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue()),
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getMongoURI(e);
    assertEquals("mongo:27017", v.get());

    // mongo url key is present
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Mongo.MONGO_URL),
            SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getMongoURI(e);
    assertEquals("mongo:27017", v.get());

    // mongo address is present
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Mongo.MONGO_ADDRESS),
            SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getMongoURI(e);
    assertEquals("mongo:27017", v.get());
  }

  @Test
  public void isRedisBackend() {
    Event e = mock(Event.class);
    // redis connection present
    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Redis.REDIS_CONNECTION),
            SemanticConventionTestUtil.buildAttributeValue("127.0.0.1:4562")));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = DbSemanticConventionUtils.isRedisBackend(e);
    assertTrue(v);

    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(OTelDbAttributes.REDIS_DB_SYSTEM_VALUE.getValue()),
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("redis:1111")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isRedisBackend(e);
    assertTrue(v);

    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue()),
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("redis:1111")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isRedisBackend(e);
    assertFalse(v);
  }

  @Test
  public void testGetRedisURI() {
    Event e = mock(Event.class);
    // redis connection present
    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Redis.REDIS_CONNECTION),
            SemanticConventionTestUtil.buildAttributeValue("127.0.0.1:4562")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getRedisURI(e);
    assertEquals("127.0.0.1:4562", v.get());

    // sql url not present, use db connection string
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("redis://127.0.0.1:4562")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getRedisURI(e);
    assertEquals("redis://127.0.0.1:4562", v.get());
  }

  @Test
  public void testIsSqlBackend() {
    Event e = mock(Event.class);

    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Sql.SQL_SQL_URL),
            SemanticConventionTestUtil.buildAttributeValue("127.0.0.1:3306")));
    when(e.getAttributes()).thenReturn(attributes);
    when(e.getEventName()).thenReturn("jdbc:mysql");
    boolean v = DbSemanticConventionUtils.isSqlBackend(e);
    assertTrue(v);

    when(e.getEventName()).thenReturn("hsql:mysql");
    v = DbSemanticConventionUtils.isSqlBackend(e);
    assertFalse(v);
  }

  @Test
  public void testIsSqlTypeBackendForOtelFormat() {
    Event e = mock(Event.class);

    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(OTelDbAttributes.DB2_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = DbSemanticConventionUtils.isSqlTypeBackendForOtelFormat(e);
    assertTrue(v);

    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isSqlTypeBackendForOtelFormat(e);
    assertFalse(v);
  }

  @Test
  public void testGetSqlURI() {
    Event e = mock(Event.class);
    // sql url present
    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Sql.SQL_SQL_URL),
            SemanticConventionTestUtil.buildAttributeValue("127.0.0.1:3306")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getSqlURI(e);
    assertEquals("127.0.0.1:3306", v.get());

    // sql url not present, use db connection string
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("mysql://127.0.0.1:3306")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getSqlURI(e);
    assertEquals("mysql://127.0.0.1:3306", v.get());

    // no sql related attributes present
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            "span.kind",
            SemanticConventionTestUtil.buildAttributeValue("clinet")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getSqlURI(e);
    assertTrue(v.isEmpty());
  }

  @Test
  public void testGetBackendURIForOtelFormat() {
    Event e = mock(Event.class);
    // connection string is present
    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("mysql://127.0.0.1:3306")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getBackendURIForOtelFormat(e);
    assertEquals("mysql://127.0.0.1:3306", v.get());

    // only ip is present
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.NET_PEER_IP.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("127.0.0.1")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getBackendURIForOtelFormat(e);
    assertEquals("127.0.0.1", v.get());

    // ip & host present
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.NET_PEER_IP.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("127.0.0.1"),
            OTelDbAttributes.NET_PEER_NAME.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("mysql.example.com")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getBackendURIForOtelFormat(e);
    assertEquals("mysql.example.com", v.get());

    // host & port present
    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.NET_PEER_IP.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("127.0.0.1"),
            OTelDbAttributes.NET_PEER_NAME.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("mysql.example.com"),
            OTelDbAttributes.NET_PEER_PORT.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("3306")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getBackendURIForOtelFormat(e);
    assertEquals("mysql.example.com:3306", v.get());
  }

  @Test
  public void testGetDbTypeForOtelFormat() {
    Event e = mock(Event.class);
    Attributes attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDbTypeForOtelFormat(e);
    assertEquals(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue(), v.get());

    attributes = SemanticConventionTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(null);
    when(e.getEnrichedAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getDbTypeForOtelFormat(e);
    assertEquals(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue(), v.get());

    when(e.getAttributes()).thenReturn(null);
    when(e.getEnrichedAttributes()).thenReturn(null);
    v = DbSemanticConventionUtils.getDbTypeForOtelFormat(e);
    assertTrue(v.isEmpty());
  }
}
