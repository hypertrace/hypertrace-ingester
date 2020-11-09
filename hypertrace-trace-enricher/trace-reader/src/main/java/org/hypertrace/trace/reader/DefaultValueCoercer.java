package org.hypertrace.trace.reader;

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;

class DefaultValueCoercer implements ValueCoercer {
  static final DefaultValueCoercer INSTANCE = new DefaultValueCoercer();

  private DefaultValueCoercer() {}

  @Override
  public Optional<LiteralValue> toLiteral(String stringValue, AttributeKind attributeKind) {

    switch (attributeKind) {
      case TYPE_DOUBLE:
        return tryParseDouble(stringValue).map(this::doubleLiteral);
      case TYPE_INT64:
        return tryParseLong(stringValue).map(this::longLiteral);
      case TYPE_BOOL:
        return tryParseBoolean(stringValue).map(this::booleanLiteral);
      case TYPE_STRING:
      case TYPE_BYTES: // Treat bytes and string the same
        return Optional.of(stringLiteral(stringValue));
      case TYPE_TIMESTAMP:
        return tryParseLong(stringValue)
            .or(() -> tryParseTimestamp(stringValue))
            .map(this::longLiteral);
      default:
        return Optional.empty();
    }
  }

  @Override
  public Optional<LiteralValue> toLiteral(Double doubleValue, AttributeKind attributeKind) {
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

  private LiteralValue stringLiteral(@Nonnull String stringValue) {
    return LiteralValue.newBuilder().setStringValue(stringValue).build();
  }

  private LiteralValue longLiteral(@Nonnull Number number) {
    return LiteralValue.newBuilder().setIntValue(number.longValue()).build();
  }

  private LiteralValue doubleLiteral(@Nonnull Number number) {
    return LiteralValue.newBuilder().setFloatValue(number.doubleValue()).build();
  }

  private LiteralValue booleanLiteral(boolean booleanValue) {
    return LiteralValue.newBuilder().setBooleanValue(booleanValue).build();
  }

  private Optional<Long> tryParseLong(@Nullable String intString) {
    try {
      return Optional.of(Long.valueOf(requireNonNull(intString)));
    } catch (Throwable ignored) {
      return Optional.empty();
    }
  }

  private Optional<Double> tryParseDouble(@Nullable String doubleString) {
    try {
      return Optional.of(Double.valueOf(requireNonNull(doubleString)));
    } catch (Throwable ignored) {
      return Optional.empty();
    }
  }

  private Optional<Boolean> tryParseBoolean(@Nullable String booleanString) {
    if ("true".equalsIgnoreCase(booleanString)) {
      return Optional.of(Boolean.TRUE);
    }
    if ("false".equalsIgnoreCase(booleanString)) {
      return Optional.of(Boolean.FALSE);
    }
    return Optional.empty();
  }

  private Optional<Long> tryParseTimestamp(@Nullable String dateString) {
    try {
      return Optional.of(Instant.parse(requireNonNull(dateString))).map(Instant::toEpochMilli);
    } catch (Throwable ignored) {
      return Optional.empty();
    }
  }
}
