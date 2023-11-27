package org.hypertrace.trace.accessor.entities;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface AttributeValueConverter {
  Logger LOG = LoggerFactory.getLogger(AttributeValueConverter.class);

  static Optional<AttributeValue> convertToAttributeValue(LiteralValue literalValue) {
    switch (literalValue.getValueCase()) {
      case STRING_VALUE:
        return attributeValueOptional(Value.newBuilder().setString(literalValue.getStringValue()));
      case BOOLEAN_VALUE:
        return attributeValueOptional(
            Value.newBuilder().setBoolean(literalValue.getBooleanValue()));
      case FLOAT_VALUE:
        return attributeValueOptional(Value.newBuilder().setDouble(literalValue.getFloatValue()));
      case INT_VALUE:
        return attributeValueOptional(Value.newBuilder().setLong(literalValue.getIntValue()));
      case VALUE_NOT_SET:
        return Optional.empty();
      default:
        LOG.error("Unexpected literal value case: " + literalValue.getValueCase());
        return Optional.empty();
    }
  }

  private static Optional<AttributeValue> attributeValueOptional(Value.Builder value) {
    return Optional.of(AttributeValue.newBuilder().setValue(value).build());
  }
}
