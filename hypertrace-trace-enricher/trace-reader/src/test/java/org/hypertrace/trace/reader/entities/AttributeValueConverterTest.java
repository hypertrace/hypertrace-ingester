package org.hypertrace.trace.reader.entities;

import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.booleanLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.doubleLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.longLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.stringLiteral;
import static org.hypertrace.trace.reader.entities.AttributeValueConverter.convertToAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.booleanAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.doubleAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.longAttributeValue;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.stringAttributeValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.junit.jupiter.api.Test;

class AttributeValueConverterTest {

  @Test
  void convertsStringValue() {
    assertEquals(
        stringAttributeValue("foo"), convertToAttributeValue(stringLiteral("foo")).blockingGet());
    assertEquals(
        stringAttributeValue(""), convertToAttributeValue(stringLiteral("")).blockingGet());
  }

  @Test
  void convertsBooleanValue() {
    assertEquals(
        booleanAttributeValue(true), convertToAttributeValue(booleanLiteral(true)).blockingGet());
    assertEquals(
        booleanAttributeValue(false), convertToAttributeValue(booleanLiteral(false)).blockingGet());
  }

  @Test
  void convertsIntValue() {
    assertEquals(longAttributeValue(0), convertToAttributeValue(longLiteral(0)).blockingGet());
    assertEquals(longAttributeValue(100), convertToAttributeValue(longLiteral(100)).blockingGet());
  }

  @Test
  void convertsFloatValue() {
    assertEquals(
        doubleAttributeValue(10.4), convertToAttributeValue(doubleLiteral(10.4)).blockingGet());
    assertEquals(
        doubleAttributeValue(-3.5), convertToAttributeValue(doubleLiteral(-3.5)).blockingGet());
  }

  @Test
  void errorsOnUnknownValue() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> convertToAttributeValue(LiteralValue.getDefaultInstance()).blockingGet());
  }
}
