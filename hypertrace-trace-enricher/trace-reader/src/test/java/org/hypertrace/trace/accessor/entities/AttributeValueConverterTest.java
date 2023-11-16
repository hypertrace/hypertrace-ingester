package org.hypertrace.trace.accessor.entities;

import static org.hypertrace.trace.accessor.entities.AttributeValueConverter.convertToAttributeValue;
import static org.hypertrace.trace.accessor.entities.AttributeValueUtil.booleanAttributeValue;
import static org.hypertrace.trace.accessor.entities.AttributeValueUtil.doubleAttributeValue;
import static org.hypertrace.trace.accessor.entities.AttributeValueUtil.longAttributeValue;
import static org.hypertrace.trace.accessor.entities.AttributeValueUtil.stringAttributeValue;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.booleanLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.doubleLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.longLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.stringLiteral;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.junit.jupiter.api.Test;

class AttributeValueConverterTest {

  @Test
  void convertsStringValue() {
    assertEquals(
        stringAttributeValue("foo"), convertToAttributeValue(stringLiteral("foo")).get());
    assertEquals(
        stringAttributeValue(""), convertToAttributeValue(stringLiteral("")).get());
  }

  @Test
  void convertsBooleanValue() {
    assertEquals(
        booleanAttributeValue(true), convertToAttributeValue(booleanLiteral(true)).get());
    assertEquals(
        booleanAttributeValue(false), convertToAttributeValue(booleanLiteral(false)).get());
  }

  @Test
  void convertsIntValue() {
    assertEquals(longAttributeValue(0), convertToAttributeValue(longLiteral(0)).get());
    assertEquals(longAttributeValue(100), convertToAttributeValue(longLiteral(100)).get());
  }

  @Test
  void convertsFloatValue() {
    assertEquals(
        doubleAttributeValue(10.4), convertToAttributeValue(doubleLiteral(10.4)).get());
    assertEquals(
        doubleAttributeValue(-3.5), convertToAttributeValue(doubleLiteral(-3.5)).get());
  }

  @Test
  void emptyOnUnknownValue() {
    assertTrue(convertToAttributeValue(LiteralValue.getDefaultInstance()).isEmpty());
  }
}
