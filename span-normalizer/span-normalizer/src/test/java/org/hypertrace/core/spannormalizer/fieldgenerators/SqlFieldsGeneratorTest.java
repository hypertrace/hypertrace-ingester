package org.hypertrace.core.spannormalizer.fieldgenerators;

import static org.hypertrace.core.span.constants.v1.Sql.SQL_DB_TYPE;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_PARAMS;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_QUERY;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_SQL_URL;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_STATE;
import static org.hypertrace.core.spannormalizer.utils.TestUtils.createKeyValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.sql.Sql;
import org.hypertrace.core.semantic.convention.constants.db.OTelDbSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SqlFieldsGeneratorTest {

  @Test
  public void testSqlFieldsGeneration() {
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap =
        Map.of(
            RawSpanConstants.getValue(SQL_QUERY),
            createKeyValue("select name, email from users where id = ? and email = ?;"),
            RawSpanConstants.getValue(SQL_DB_TYPE),
            createKeyValue("mysql"),
            RawSpanConstants.getValue(SQL_SQL_URL),
            createKeyValue("mysql:3306"),
            RawSpanConstants.getValue(SQL_PARAMS),
            createKeyValue("[1]=897,[2]=foo@bar.com"),
            RawSpanConstants.getValue(SQL_STATE),
            createKeyValue("01002"));

    SqlFieldsGenerator sqlFieldsGenerator = new SqlFieldsGenerator();
    Event.Builder eventBuilder = Event.newBuilder();
    Sql.Builder sqlBuilder = sqlFieldsGenerator.getProtocolBuilder(eventBuilder);

    Assertions.assertSame(eventBuilder.getSqlBuilder(), sqlBuilder);

    tagsMap.forEach(
        (key, keyValue) ->
            sqlFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap));

    assertEquals(
        "select name, email from users where id = ? and email = ?;", sqlBuilder.getQuery());
    assertEquals("mysql", sqlBuilder.getDbType());
    assertEquals("mysql:3306", sqlBuilder.getUrl());
    assertEquals("[1]=897,[2]=foo@bar.com", sqlBuilder.getParams());
    assertEquals("01002", sqlBuilder.getSqlstate());
  }

  @Test
  public void testSqlFieldsGenerationOtelFormat() {
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap =
        Map.of(
            OTelDbSemanticConventions.DB_STATEMENT.getValue(),
            createKeyValue("select name, email from users where id = ? and email = ?;"),
            OTelDbSemanticConventions.DB_SYSTEM.getValue(),
            createKeyValue("mysql"));

    SqlFieldsGenerator sqlFieldsGenerator = new SqlFieldsGenerator();
    Event.Builder eventBuilder = Event.newBuilder();
    Sql.Builder sqlBuilder = sqlFieldsGenerator.getProtocolBuilder(eventBuilder);

    Assertions.assertSame(eventBuilder.getSqlBuilder(), sqlBuilder);

    tagsMap.forEach(
        (key, keyValue) ->
            sqlFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap));

    assertEquals(
        "select name, email from users where id = ? and email = ?;", sqlBuilder.getQuery());
    assertEquals("mysql", sqlBuilder.getDbType());
  }

  @Test
  public void testPopulateOtherFields() {
    SqlFieldsGenerator sqlFieldsGenerator = new SqlFieldsGenerator();

    Event.Builder eventBuilder = Event.newBuilder();

    Map<String, AttributeValue> map =
        Map.of(
            OTelDbSemanticConventions.DB_SYSTEM.getValue(),
            AttributeValue.newBuilder()
                .setValue(OTelDbSemanticConventions.MYSQL_DB_SYSTEM_VALUE.getValue())
                .build(),
            OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(),
            AttributeValue.newBuilder()
                .setValue(
                    "Server=shopdb.example.com;Database=ShopDb;Uid=billing_user;TableCache=true;UseCompression=True;MinimumPoolSize=10;MaximumPoolSize=50;")
                .build(),
            OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
            AttributeValue.newBuilder().setValue("mysql.example.com").build(),
            OTelSpanSemanticConventions.NET_PEER_PORT.getValue(),
            AttributeValue.newBuilder().setValue("3306").build());
    sqlFieldsGenerator.populateOtherFields(eventBuilder, map);
    assertEquals("mysql.example.com:3306", eventBuilder.getSqlBuilder().getUrl());
  }
}
