package org.hypertrace.core.spannormalizer.fieldgenerators;

import static org.hypertrace.core.span.constants.v1.Sql.SQL_DB_TYPE;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_PARAMS;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_QUERY;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_SQL_URL;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_STATE;
import static org.hypertrace.core.spannormalizer.utils.TestUtils.createKeyValue;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.Map;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.sql.Sql;
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
            RawSpanConstants.getValue(SQL_DB_TYPE), createKeyValue("mysql"),
            RawSpanConstants.getValue(SQL_SQL_URL), createKeyValue("mysql:3306"),
            RawSpanConstants.getValue(SQL_PARAMS), createKeyValue("[1]=897,[2]=foo@bar.com"),
            RawSpanConstants.getValue(SQL_STATE), createKeyValue("01002"));

    SqlFieldsGenerator sqlFieldsGenerator = new SqlFieldsGenerator();
    Event.Builder eventBuilder = Event.newBuilder();
    Sql.Builder sqlBuilder = sqlFieldsGenerator.getProtocolBuilder(eventBuilder);

    Assertions.assertSame(eventBuilder.getSqlBuilder(), sqlBuilder);

    tagsMap.forEach(
        (key, keyValue) ->
            sqlFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap));

    Assertions.assertEquals(
        "select name, email from users where id = ? and email = ?;", sqlBuilder.getQuery());
    Assertions.assertEquals("mysql", sqlBuilder.getDbType());
    Assertions.assertEquals("mysql:3306", sqlBuilder.getUrl());
    Assertions.assertEquals("[1]=897,[2]=foo@bar.com", sqlBuilder.getParams());
    Assertions.assertEquals("01002", sqlBuilder.getSqlstate());
  }
}
