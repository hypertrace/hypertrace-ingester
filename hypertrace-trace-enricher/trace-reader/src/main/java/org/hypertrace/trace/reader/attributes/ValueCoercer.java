package org.hypertrace.trace.reader.attributes;

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;

public interface ValueCoercer {

  static Optional<LiteralValue> toLiteral(String stringValue, AttributeKind attributeKind) {

    switch (attributeKind) {
      case TYPE_DOUBLE:
        return tryParseDouble(stringValue).map(ValueCoercer::doubleLiteral);
      case TYPE_INT64:
        return tryParseLong(stringValue).map(ValueCoercer::longLiteral);
      case TYPE_BOOL:
        return tryParseBoolean(stringValue).map(ValueCoercer::booleanLiteral);
      case TYPE_STRING:
      case TYPE_BYTES: // Treat bytes and string the same
        return Optional.of(stringLiteral(stringValue));
      case TYPE_TIMESTAMP:
        return tryParseLong(stringValue)
            .or(() -> tryParseTimestamp(stringValue))
            .map(ValueCoercer::longLiteral);
      default:
        return Optional.empty();
    }
  }

  static Optional<LiteralValue> toLiteral(Double doubleValue, AttributeKind attributeKind) {
    switch (attributeKind) {
      case TYPE_DOUBLE:
        return Optional.of(doubleLiteral(doubleValue));
      case TYPE_TIMESTAMP:
      case TYPE_INT64: // Timestamp and long both convert the same
        return Optional.of(longLiteral(doubleValue));
      case TYPE_STRING:
      case TYPE_BYTES: // Treat bytes and string the same
        return Optional.of(stringLiteral(doubleValue.toString()));

      default:
        return Optional.empty();
    }
  }

  static Optional<LiteralValue> toLiteral(Long longValue, AttributeKind attributeKind) {
    switch (attributeKind) {
      case TYPE_DOUBLE:
        return Optional.of(doubleLiteral(longValue));
      case TYPE_TIMESTAMP:
      case TYPE_INT64: // Timestamp and long both convert the same
        return Optional.of(longLiteral(longValue));
      case TYPE_STRING:
      case TYPE_BYTES: // Treat bytes and string the same
        return Optional.of(stringLiteral(longValue.toString()));
      default:
        return Optional.empty();
    }
  }

  static Optional<String> convertToString(LiteralValue literalValue) {
    switch (literalValue.getValueCase()) {
      case INT_VALUE:
        return Optional.of(String.valueOf(literalValue.getIntValue()));
      case STRING_VALUE:
        return Optional.of(literalValue.getStringValue());
      case FLOAT_VALUE:
        return Optional.of(String.valueOf(literalValue.getFloatValue()));
      case BOOLEAN_VALUE:
        return Optional.of(String.valueOf(literalValue.getBooleanValue()));
      case VALUE_NOT_SET:
      default:
        return Optional.empty();
    }
  }

  private static LiteralValue stringLiteral(@Nonnull String stringValue) {
    return LiteralValue.newBuilder().setStringValue(stringValue).build();
  }

  private static LiteralValue longLiteral(@Nonnull Number number) {
    return LiteralValue.newBuilder().setIntValue(number.longValue()).build();
  }

  private static LiteralValue doubleLiteral(@Nonnull Number number) {
    return LiteralValue.newBuilder().setFloatValue(number.doubleValue()).build();
  }

  private static LiteralValue booleanLiteral(boolean booleanValue) {
    return LiteralValue.newBuilder().setBooleanValue(booleanValue).build();
  }

  private static Optional<Long> tryParseLong(@Nullable String intString) {
    try {
      return Optional.of(Long.valueOf(requireNonNull(intString)));
    } catch (Throwable ignored) {
      return Optional.empty();
    }
  }

  private static Optional<Double> tryParseDouble(@Nullable String doubleString) {
    try {
      return Optional.of(Double.valueOf(requireNonNull(doubleString)));
    } catch (Throwable ignored) {
      return Optional.empty();
    }
  }

  private static Optional<Boolean> tryParseBoolean(@Nullable String booleanString) {
    if ("true".equalsIgnoreCase(booleanString)) {
      return Optional.of(Boolean.TRUE);
    }
    if ("false".equalsIgnoreCase(booleanString)) {
      return Optional.of(Boolean.FALSE);
    }
    return Optional.empty();
  }

  private static Optional<Long> tryParseTimestamp(@Nullable String dateString) {
    try {
      return Optional.of(Instant.parse(requireNonNull(dateString))).map(Instant::toEpochMilli);
    } catch (Throwable ignored) {
      return Optional.empty();
    }
  }
}
