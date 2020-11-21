package org.hypertrace.attributeutils.db;

import static org.hypertrace.attributeutils.AttributeTestUtil.buildAttributeValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.attributeutils.AttributeTestUtil;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Mongo;
import org.hypertrace.core.span.constants.v1.Redis;
import org.hypertrace.core.span.constants.v1.Sql;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link DbAttributeUtils}
 */
public class DbAttributeUtilsTest {

  @Test
  public void isMongoBackend() {
    Event e = mock(Event.class);
    // otel format
    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue()),
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = DbAttributeUtils.isMongoBackend(e);
    assertTrue(v);

    // otel format, dbsystem is not mongo
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            buildAttributeValue(OTelDbAttributes.MYSQL_DB_SYSTEM_VALUE.getValue()),
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.isMongoBackend(e);
    assertFalse(v);

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Mongo.MONGO_ADDRESS),
            buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.isMongoBackend(e);
    assertTrue(v);

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Mongo.MONGO_URL),
            buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.isMongoBackend(e);
    assertTrue(v);

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            "span.kind",
            buildAttributeValue("client")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.isMongoBackend(e);
    assertFalse(v);
  }


  @Test
  public void testGetMongoURI() {
    Event e = mock(Event.class);
    // otel format
    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue()),
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbAttributeUtils.getMongoURI(e);
    assertEquals("mongo:27017", v.get());

    // mongo url key is present
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Mongo.MONGO_URL),
            buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.getMongoURI(e);
    assertEquals("mongo:27017", v.get());

    // mongo address is present
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Mongo.MONGO_ADDRESS),
            buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.getMongoURI(e);
    assertEquals("mongo:27017", v.get());
  }

  @Test
  public void isRedisBackend() {
    Event e = mock(Event.class);
    // redis connection present
    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Redis.REDIS_CONNECTION),
            buildAttributeValue("127.0.0.1:4562")));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = DbAttributeUtils.isRedisBackend(e);
    assertTrue(v);

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            buildAttributeValue(OTelDbAttributes.REDIS_DB_SYSTEM_VALUE.getValue()),
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.isRedisBackend(e);
    assertTrue(v);

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue()),
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.isRedisBackend(e);
    assertFalse(v);
  }

  @Test
  public void testGetRedisURI() {
    Event e = mock(Event.class);
    // redis connection present
    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Redis.REDIS_CONNECTION),
            buildAttributeValue("127.0.0.1:4562")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbAttributeUtils.getRedisURI(e);
    assertEquals("127.0.0.1:4562", v.get());

    // sql url not present, use db connection string
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            buildAttributeValue("mysql://127.0.0.1:4562")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.getRedisURI(e);
    assertEquals("mysql://127.0.0.1:4562", v.get());
  }

  @Test
  public void testIsSqlBackend() {
    Event e = mock(Event.class);

    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Sql.SQL_SQL_URL),
            buildAttributeValue("127.0.0.1:3306")));
    when(e.getAttributes()).thenReturn(attributes);
    when(e.getEventName()).thenReturn("jdbc:mysql");
    boolean v = DbAttributeUtils.isSqlBackend(e);
    assertTrue(v);

    when(e.getEventName()).thenReturn("hsql:mysql");
    v = DbAttributeUtils.isSqlBackend(e);
    assertFalse(v);
  }

  @Test
  public void testIsSqlTypeBackendForOtelFormat() {
    Event e = mock(Event.class);

    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            buildAttributeValue(OTelDbAttributes.DB2_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = DbAttributeUtils.isSqlTypeBackendForOtelFormat(e);
    assertTrue(v);

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.isSqlTypeBackendForOtelFormat(e);
    assertFalse(v);
  }

  @Test
  public void testGetSqlURI() {
    Event e = mock(Event.class);
    // sql url present
    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Sql.SQL_SQL_URL),
            buildAttributeValue("127.0.0.1:3306")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbAttributeUtils.getSqlURI(e);
    assertEquals("127.0.0.1:3306", v.get());

    // sql url not present, use db connection string
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            buildAttributeValue("mysql://127.0.0.1:3306")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.getSqlURI(e);
    assertEquals("mysql://127.0.0.1:3306", v.get());

    // no sql related attributes present
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            "span.kind",
            buildAttributeValue("clinet")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.getSqlURI(e);
    assertTrue(v.isEmpty());
  }

  @Test
  public void testGetBackendURIForOtelFormat() {
    Event e = mock(Event.class);
    // connection string is present
    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_CONNECTION_STRING.getValue(),
            buildAttributeValue("mysql://127.0.0.1:3306")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbAttributeUtils.getBackendURIForOtelFormat(e);
    assertEquals("mysql://127.0.0.1:3306", v.get());

    // only ip is present
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.NET_PEER_IP.getValue(),
            buildAttributeValue("127.0.0.1")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.getBackendURIForOtelFormat(e);
    assertEquals("127.0.0.1", v.get());

    // ip & host present
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.NET_PEER_IP.getValue(),
            buildAttributeValue("127.0.0.1"),
            OTelDbAttributes.NET_PEER_NAME.getValue(),
            buildAttributeValue("mysql.example.com")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.getBackendURIForOtelFormat(e);
    assertEquals("mysql.example.com", v.get());

    // host & port present
    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.NET_PEER_IP.getValue(),
            buildAttributeValue("127.0.0.1"),
            OTelDbAttributes.NET_PEER_NAME.getValue(),
            buildAttributeValue("mysql.example.com"),
            OTelDbAttributes.NET_PEER_PORT.getValue(),
            buildAttributeValue("3306")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.getBackendURIForOtelFormat(e);
    assertEquals("mysql.example.com:3306", v.get());
  }

  @Test
  public void testGetDbTypeForOtelFormat() {
    Event e = mock(Event.class);
    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbAttributeUtils.getDbTypeForOtelFormat(e);
    assertEquals(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue(), v.get());

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelDbAttributes.DB_SYSTEM.getValue(),
            buildAttributeValue(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(null);
    when(e.getEnrichedAttributes()).thenReturn(attributes);
    v = DbAttributeUtils.getDbTypeForOtelFormat(e);
    assertEquals(OTelDbAttributes.MONGODB_DB_SYSTEM_VALUE.getValue(), v.get());

    when(e.getAttributes()).thenReturn(null);
    when(e.getEnrichedAttributes()).thenReturn(null);
    v = DbAttributeUtils.getDbTypeForOtelFormat(e);
    assertTrue(v.isEmpty());
  }
}
