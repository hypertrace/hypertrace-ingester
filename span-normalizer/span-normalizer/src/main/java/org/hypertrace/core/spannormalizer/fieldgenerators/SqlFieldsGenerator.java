package org.hypertrace.core.spannormalizer.fieldgenerators;

import static org.hypertrace.core.span.constants.v1.Sql.SQL_DB_TYPE;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_PARAMS;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_QUERY;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_SQL_URL;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_STATE;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.sql.Sql;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.hypertrace.semantic.convention.utils.db.OTelDbSemanticConventions;

public class SqlFieldsGenerator extends ProtocolFieldsGenerator<Sql.Builder> {
  private static final Map<String, FieldGenerator<Sql.Builder>> FIELD_GENERATOR_MAP =
      initializeFieldGenerators();

  private static Map<String, FieldGenerator<Sql.Builder>> initializeFieldGenerators() {
    Map<String, FieldGenerator<Sql.Builder>> fieldGeneratorMap = new HashMap<>();

    // set sql query
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(SQL_QUERY),
        (key, keyValue, builder, tagsMap) -> builder.setQuery(keyValue.getVStr()));
    fieldGeneratorMap.put(
        OTelDbSemanticConventions.DB_STATEMENT.getValue(),
        (key, keyValue, builder, tagsMap) -> builder.setQuery(keyValue.getVStr()));
    // db type
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(SQL_DB_TYPE),
        (key, keyValue, builder, tagsMap) -> builder.setDbType(keyValue.getVStr()));
    fieldGeneratorMap.put(
        OTelDbSemanticConventions.DB_SYSTEM.getValue(),
        (key, keyValue, builder, tagsMap) -> builder.setDbType(keyValue.getVStr()));

    fieldGeneratorMap.put(
        RawSpanConstants.getValue(SQL_SQL_URL),
        (key, keyValue, builder, tagsMap) -> builder.setUrl(keyValue.getVStr()));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(SQL_PARAMS),
        (key, keyValue, builder, tagsMap) -> builder.setParams(keyValue.getVStr()));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(SQL_STATE),
        (key, keyValue, builder, tagsMap) -> builder.setSqlstate(keyValue.getVStr()));


    return fieldGeneratorMap;
  }

  @Override
  protected Sql.Builder getProtocolBuilder(Event.Builder eventBuilder) {
    return eventBuilder.getSqlBuilder();
  }

  @Override
  protected Map<String, FieldGenerator<Sql.Builder>> getFieldGeneratorMap() {
    return FIELD_GENERATOR_MAP;
  }

  protected void populateOtherFields(Event.Builder eventBuilder, final Map<String, AttributeValue> attributeValueMap) {
    maybePopulateSqlUrlForOtelSpan(eventBuilder, attributeValueMap);
  }

  protected void maybePopulateSqlUrlForOtelSpan(
      Event.Builder eventBuilder,
      final Map<String, AttributeValue> attributeFieldMap) {
    if (DbSemanticConventionUtils.isSqlTypeBackendForOtelFormat(attributeFieldMap)) {
      Optional<String> sqlUrl = DbSemanticConventionUtils.getBackendURIForOtelFormat(attributeFieldMap);
      sqlUrl.ifPresent(s -> eventBuilder.getSqlBuilder().setUrl(s));
    }
  }
}
