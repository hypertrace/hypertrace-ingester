package org.hypertrace.trace.reader.entities;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Value;

class AttributeValueConverter {

  public Single<AttributeValue> convert(LiteralValue literalValue) {
    switch (literalValue.getValueCase()) {
      case STRING_VALUE:
        return attributeValueSingle(Value.newBuilder().setString(literalValue.getStringValue()));
      case BOOLEAN_VALUE:
        return attributeValueSingle(Value.newBuilder().setBoolean(literalValue.getBooleanValue()));
      case FLOAT_VALUE:
        return attributeValueSingle(Value.newBuilder().setDouble(literalValue.getFloatValue()));
      case INT_VALUE:
        return attributeValueSingle(Value.newBuilder().setLong(literalValue.getIntValue()));
      case VALUE_NOT_SET:
      default:
        return Single.error(
            new UnsupportedOperationException(
                "Unexpected literal value case: " + literalValue.getValueCase()));
    }
  }

  private Single<AttributeValue> attributeValueSingle(Value.Builder value) {
    return Single.just(AttributeValue.newBuilder().setValue(value).build());
  }
}
