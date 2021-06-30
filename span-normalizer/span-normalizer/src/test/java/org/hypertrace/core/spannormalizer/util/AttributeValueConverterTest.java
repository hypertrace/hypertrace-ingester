package org.hypertrace.core.spannormalizer.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import org.hypertrace.core.datamodel.AttributeValue;
import org.junit.jupiter.api.Test;

public class AttributeValueConverterTest {

  @Test
  public void testConvertAttributeToKeyValue() {
    String val = "sample value";
    AttributeValue attributeValue = AttributeValue.newBuilder().setValue(val).build();
    assertEquals(val, AttributeValueCreator.convertAttributeToKeyValue(attributeValue).getVStr());
    assertEquals(
        JaegerSpanInternalModel.ValueType.STRING,
        AttributeValueCreator.convertAttributeToKeyValue(attributeValue).getVType());
  }
}
