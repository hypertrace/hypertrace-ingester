package org.hypertrace.semantic.convention.utils.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.semantic.convention.constants.db.OTelDbSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Mongo;
import org.hypertrace.core.span.constants.v1.Redis;
import org.hypertrace.core.span.constants.v1.Sql;
import org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil;
import org.junit.jupiter.api.Test;

/** Unit test for {@link DbSemanticConventionUtils} */
public class DbSemanticConventionUtilsTest {

  @Test
  public void isMongoBackend() {
    Event e = mock(Event.class);

    // otel format, dbsystem is not mongo
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.DB_SYSTEM.getValue(),
                SemanticConventionTestUtil.buildAttributeValue(
                    OTelDbSemanticConventions.MYSQL_DB_SYSTEM_VALUE.getValue()),
                OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = DbSemanticConventionUtils.isMongoBackend(e);
    assertFalse(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Mongo.MONGO_ADDRESS),
                SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isMongoBackend(e);
    assertTrue(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Mongo.MONGO_URL),
                SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isMongoBackend(e);
    assertTrue(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of("span.kind", SemanticConventionTestUtil.buildAttributeValue("client")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isMongoBackend(e);
    assertFalse(v);
  }

  @Test
  public void testGetMongoURI() {
    Event e = mock(Event.class);
    // mongo url key is present
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Mongo.MONGO_URL),
                SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getMongoURI(e);
    assertEquals("mongo:27017", v.get());

    // mongo address is present
    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Mongo.MONGO_ADDRESS),
                SemanticConventionTestUtil.buildAttributeValue("mongo:27017")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getMongoURI(e);
    assertEquals("mongo:27017", v.get());
  }

  @Test
  public void testGetMongoOperation() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Mongo.MONGO_OPERATION),
                SemanticConventionTestUtil.buildAttributeValue("find")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDbOperationForMongo(e);
    assertEquals("find", v.get());
  }

  @Test
  public void testGetMongoDestination() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.MONGODB_COLLECTION.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("testCollection"),
                OTelDbSemanticConventions.DB_NAME.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("customer")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDestinationForMongo(e);
    assertEquals("customer.testCollection", v.get());
  }

  @Test
  public void testIfMongoOperationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(DbSemanticConventionUtils.getDbOperationForMongo(e).isEmpty());
  }

  @Test
  public void testIfMongoDestinationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(DbSemanticConventionUtils.getDestinationForMongo(e).isEmpty());
  }

  @Test
  public void isRedisBackend() {
    Event e = mock(Event.class);
    // redis connection present
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Redis.REDIS_CONNECTION),
                SemanticConventionTestUtil.buildAttributeValue("127.0.0.1:4562")));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = DbSemanticConventionUtils.isRedisBackend(e);
    assertTrue(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.DB_SYSTEM.getValue(),
                SemanticConventionTestUtil.buildAttributeValue(
                    OTelDbSemanticConventions.REDIS_DB_SYSTEM_VALUE.getValue()),
                OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("redis:1111")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isRedisBackend(e);
    assertTrue(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.DB_SYSTEM.getValue(),
                SemanticConventionTestUtil.buildAttributeValue(
                    OTelDbSemanticConventions.MONGODB_DB_SYSTEM_VALUE.getValue()),
                OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("redis:1111")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isRedisBackend(e);
    assertFalse(v);
  }

  @Test
  public void testGetRedisURI() {
    Event e = mock(Event.class);
    // redis connection present
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Redis.REDIS_CONNECTION),
                SemanticConventionTestUtil.buildAttributeValue("127.0.0.1:4562")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getRedisURI(e);
    assertEquals("127.0.0.1:4562", v.get());
  }

  @Test
  public void testGetRedisOperation() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Redis.REDIS_COMMAND),
                SemanticConventionTestUtil.buildAttributeValue("GET")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDbOperationForRedis(e);
    assertEquals("GET", v.get());
  }

  @Test
  public void testGetRedisDestination() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.REDIS_DB_INDEX.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("15")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDestinationForRedis(e);
    assertEquals("15", v.get());
  }

  @Test
  public void testIfRedisOperationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(DbSemanticConventionUtils.getDbOperationForRedis(e).isEmpty());
  }

  @Test
  public void testIfRedisDestinationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(DbSemanticConventionUtils.getDestinationForRedis(e).isEmpty());
  }

  @Test
  public void testGetCassandraURI() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("jdbc:cassandra://localhost:9000")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getCassandraURI(e);
    assertEquals("localhost:9000", v.get());
  }

  @Test
  public void testGetCassandraOperation() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.DB_OPERATION.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("select")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDbOperation(e);
    assertEquals("select", v.get());
  }

  @Test
  public void testGetCassandraDestination() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.CASSANDRA_TABLE_NAME.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("payment"),
                OTelDbSemanticConventions.CASSANDRA_DB_NAME.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("customer")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDestinationForCassandra(e);
    assertEquals("customer.payment", v.get());
  }

  @Test
  public void testIfCassandraOperationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(DbSemanticConventionUtils.getDbOperation(e).isEmpty());
  }

  @Test
  public void testIfCassandraDestinationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(DbSemanticConventionUtils.getDestinationForCassandra(e).isEmpty());
  }

  @Test
  public void testGetElasticsearchURI() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.ELASTICSEARCH_URL.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("localhost:9000")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getElasticSearchURI(e);
    assertEquals("localhost:9000", v.get());
  }

  @Test
  public void testGetElasticSearchOperation() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.DB_OPERATION.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("GET")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDbOperationForElasticSearch(e);
    assertEquals("GET", v.get());
  }

  @Test
  public void testGetElasticSearchDestination() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.ELASTICSEARCH_REQUEST_INDICES.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("test-index")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDestinationForElasticsearch(e);
    assertEquals("test-index", v.get());
  }

  @Test
  public void testIfElasticSearchOperationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(DbSemanticConventionUtils.getDbOperationForElasticSearch(e).isEmpty());
  }

  @Test
  public void testIfElasticSearchDestinationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(DbSemanticConventionUtils.getDestinationForElasticsearch(e).isEmpty());
  }

  @Test
  public void testIsSqlBackend() {
    Event e = mock(Event.class);

    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
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

    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.DB_SYSTEM.getValue(),
                SemanticConventionTestUtil.buildAttributeValue(
                    OTelDbSemanticConventions.DB2_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = DbSemanticConventionUtils.isSqlTypeBackendForOtelFormat(e);
    assertTrue(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.DB_SYSTEM.getValue(),
                SemanticConventionTestUtil.buildAttributeValue(
                    OTelDbSemanticConventions.MONGODB_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.isSqlTypeBackendForOtelFormat(e);
    assertFalse(v);
  }

  @Test
  public void testGetSqlURI() {
    Event e = mock(Event.class);
    // sql url present
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Sql.SQL_SQL_URL),
                SemanticConventionTestUtil.buildAttributeValue("jdbc:mysql://mysql:3306/shop")));
    when(e.getAttributes()).thenReturn(attributes);
    when(e.getEventName()).thenReturn("jdbc:event");
    Optional<String> v = DbSemanticConventionUtils.getSqlURI(e);
    assertEquals("jdbc:mysql://mysql:3306/shop", v.get());

    // no sql related attributes present
    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of("span.kind", SemanticConventionTestUtil.buildAttributeValue("clinet")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getSqlURI(e);
    assertTrue(v.isEmpty());

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.DB_SYSTEM.getValue(),
                SemanticConventionTestUtil.buildAttributeValue(
                    OTelDbSemanticConventions.MYSQL_DB_SYSTEM_VALUE.getValue()),
                OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("jdbc:mysql://mysql:3306/shop")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getSqlURI(e);
    assertEquals("jdbc:mysql://mysql:3306/shop", v.get());
  }

  @Test
  public void testGetSqlUrlForOtelFormat() {
    Map<String, AttributeValue> map =
        Map.of(
            OTelDbSemanticConventions.DB_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(
                OTelDbSemanticConventions.MYSQL_DB_SYSTEM_VALUE.getValue()),
            OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("jdbc:mysql://mysql:3306/shop"));
    Optional<String> v = DbSemanticConventionUtils.getSqlUrlForOtelFormat(map);
    assertEquals("jdbc:mysql://mysql:3306/shop", v.get());

    map =
        Map.of(
            OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("jdbc:mysql://mysql:3306/shop"));
    v = DbSemanticConventionUtils.getSqlUrlForOtelFormat(map);
    assertTrue(v.isEmpty());

    map =
        Map.of(
            OTelSpanSemanticConventions.NET_PEER_IP.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("127.0.0.1"),
            OTelDbSemanticConventions.DB_SYSTEM.getValue(),
            SemanticConventionTestUtil.buildAttributeValue(
                OTelDbSemanticConventions.MYSQL_DB_SYSTEM_VALUE.getValue()),
            OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(),
            SemanticConventionTestUtil.buildAttributeValue("jdbc:mysql://mysql:3306/shop"));
    v = DbSemanticConventionUtils.getSqlUrlForOtelFormat(map);
    assertEquals("127.0.0.1", v.get());
  }

  @Test
  public void testGetSqlOperation() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Sql.SQL_QUERY),
                SemanticConventionTestUtil.buildAttributeValue("SELECT * FROM DB")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDbOperationForJDBC(e);
    assertEquals("SELECT", v.get());
  }

  @Test
  public void testGetSqlDestination() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.SQL_TABLE_NAME.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("payment"),
                OTelDbSemanticConventions.DB_NAME.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("customer")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDestinationForJdbc(e);
    assertEquals("customer.payment", v.get());
  }

  @Test
  public void testIfSqlOperationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(DbSemanticConventionUtils.getDbOperationForJDBC(e).isEmpty());
  }

  @Test
  public void testIfsqlDestinationIsEmpty() {
    Event e = mock(Event.class);
    assertTrue(DbSemanticConventionUtils.getDestinationForJdbc(e).isEmpty());
  }

  @Test
  public void testGetBackendURIForOtelFormat() {
    Event e = mock(Event.class);

    // only ip is present
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelSpanSemanticConventions.NET_PEER_IP.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("127.0.0.1")));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getBackendURIForOtelFormat(e);
    assertEquals("127.0.0.1", v.get());

    // ip & host present
    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelSpanSemanticConventions.NET_PEER_IP.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("127.0.0.1"),
                OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("mysql.example.com")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getBackendURIForOtelFormat(e);
    assertEquals("mysql.example.com", v.get());

    // host & port present
    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelSpanSemanticConventions.NET_PEER_IP.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("127.0.0.1"),
                OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("mysql.example.com"),
                OTelSpanSemanticConventions.NET_PEER_PORT.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("3306")));
    when(e.getAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getBackendURIForOtelFormat(e);
    assertEquals("mysql.example.com:3306", v.get());
  }

  @Test
  public void testGetDbTypeForOtelFormat() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.DB_SYSTEM.getValue(),
                SemanticConventionTestUtil.buildAttributeValue(
                    OTelDbSemanticConventions.MONGODB_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(attributes);
    Optional<String> v = DbSemanticConventionUtils.getDbTypeForOtelFormat(e);
    assertEquals(OTelDbSemanticConventions.MONGODB_DB_SYSTEM_VALUE.getValue(), v.get());

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelDbSemanticConventions.DB_SYSTEM.getValue(),
                SemanticConventionTestUtil.buildAttributeValue(
                    OTelDbSemanticConventions.MONGODB_DB_SYSTEM_VALUE.getValue())));
    when(e.getAttributes()).thenReturn(null);
    when(e.getEnrichedAttributes()).thenReturn(attributes);
    v = DbSemanticConventionUtils.getDbTypeForOtelFormat(e);
    assertEquals(OTelDbSemanticConventions.MONGODB_DB_SYSTEM_VALUE.getValue(), v.get());

    when(e.getAttributes()).thenReturn(null);
    when(e.getEnrichedAttributes()).thenReturn(null);
    v = DbSemanticConventionUtils.getDbTypeForOtelFormat(e);
    assertTrue(v.isEmpty());
  }
}
