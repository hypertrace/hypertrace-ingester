package org.hypertrace.trace.reader.attributes;

import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.booleanLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.doubleLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.longLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.stringLiteral;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ValueCoercerTest {

  @Test
  void canCoerceFromString() {
    Assertions.assertEquals(
        Optional.of(stringLiteral("some string")),
        ValueCoercer.toLiteral("some string", AttributeKind.TYPE_STRING));

    Assertions.assertEquals(
        Optional.of(booleanLiteral(true)), ValueCoercer.toLiteral("true", AttributeKind.TYPE_BOOL));
    Assertions.assertEquals(
        Optional.of(booleanLiteral(false)),
        ValueCoercer.toLiteral("FALSE", AttributeKind.TYPE_BOOL));
    Assertions.assertEquals(
        Optional.empty(), ValueCoercer.toLiteral("dummy", AttributeKind.TYPE_BOOL));

    Assertions.assertEquals(
        Optional.of(longLiteral(42)), ValueCoercer.toLiteral("42", AttributeKind.TYPE_INT64));
    Assertions.assertEquals(
        Optional.empty(), ValueCoercer.toLiteral("dummy", AttributeKind.TYPE_INT64));
    Assertions.assertEquals(
        Optional.of(doubleLiteral(42.0)), ValueCoercer.toLiteral("42", AttributeKind.TYPE_DOUBLE));
    Assertions.assertEquals(
        Optional.empty(), ValueCoercer.toLiteral("dummy", AttributeKind.TYPE_DOUBLE));

    Assertions.assertEquals(
        Optional.of(longLiteral(1604339469114L)),
        ValueCoercer.toLiteral("1604339469114", AttributeKind.TYPE_TIMESTAMP));
    Assertions.assertEquals(
        Optional.of(longLiteral(1604339469114L)),
        ValueCoercer.toLiteral("2020-11-02T17:51:09.114Z", AttributeKind.TYPE_TIMESTAMP));
    Assertions.assertEquals(
        Optional.empty(), ValueCoercer.toLiteral("???", AttributeKind.TYPE_TIMESTAMP));

    // Unsupported
    Assertions.assertEquals(
        Optional.empty(), ValueCoercer.toLiteral("[]", AttributeKind.TYPE_STRING_ARRAY));
    Assertions.assertEquals(
        Optional.empty(), ValueCoercer.toLiteral("{}", AttributeKind.TYPE_STRING_MAP));
  }

  @Test
  void canCoerceFromDouble() {
    Assertions.assertEquals(
        Optional.of(stringLiteral("42.3")),
        ValueCoercer.toLiteral(42.3, AttributeKind.TYPE_STRING));

    Assertions.assertEquals(
        Optional.empty(), ValueCoercer.toLiteral(42.3, AttributeKind.TYPE_BOOL));

    Assertions.assertEquals(
        Optional.of(longLiteral(42)), ValueCoercer.toLiteral(42.3, AttributeKind.TYPE_INT64));
    Assertions.assertEquals(
        Optional.of(longLiteral(42)), ValueCoercer.toLiteral(42.0, AttributeKind.TYPE_INT64));
    Assertions.assertEquals(
        Optional.of(longLiteral(42)), ValueCoercer.toLiteral(42.3, AttributeKind.TYPE_TIMESTAMP));
    Assertions.assertEquals(
        Optional.of(longLiteral(1604339469114L)),
        ValueCoercer.toLiteral(1604339469114d, AttributeKind.TYPE_TIMESTAMP));

    Assertions.assertEquals(
        Optional.of(doubleLiteral(42.3)), ValueCoercer.toLiteral(42.3, AttributeKind.TYPE_DOUBLE));

    // Unsupported
    Assertions.assertEquals(
        Optional.empty(), ValueCoercer.toLiteral(42.3, AttributeKind.TYPE_STRING_ARRAY));
    Assertions.assertEquals(
        Optional.empty(), ValueCoercer.toLiteral(42.3, AttributeKind.TYPE_STRING_MAP));
  }
}
