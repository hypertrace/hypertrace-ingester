package org.hypertrace.trace.reader.entities;

import io.reactivex.rxjava3.core.Maybe;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AttributeValueConverter {
  private static final Logger LOG = LoggerFactory.getLogger(AttributeValueConverter.class);

  static Maybe<AttributeValue> convertToAttributeValue(LiteralValue literalValue) {
    switch (literalValue.getValueCase()) {
      case STRING_VALUE:
        return attributeValueMaybe(Value.newBuilder().setString(literalValue.getStringValue()));
      case BOOLEAN_VALUE:
        return attributeValueMaybe(Value.newBuilder().setBoolean(literalValue.getBooleanValue()));
      case FLOAT_VALUE:
        return attributeValueMaybe(Value.newBuilder().setDouble(literalValue.getFloatValue()));
      case INT_VALUE:
        return attributeValueMaybe(Value.newBuilder().setLong(literalValue.getIntValue()));
      case VALUE_NOT_SET:
        return Maybe.empty();
      default:
        LOG.error("Unexpected literal value case: " + literalValue.getValueCase());
        return Maybe.empty();
    }
  }

  private static Maybe<AttributeValue> attributeValueMaybe(Value.Builder value) {
    return Maybe.just(AttributeValue.newBuilder().setValue(value).build());
  }
}
