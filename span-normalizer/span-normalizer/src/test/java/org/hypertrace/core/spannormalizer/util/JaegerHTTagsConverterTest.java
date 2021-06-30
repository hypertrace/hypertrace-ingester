package org.hypertrace.core.spannormalizer.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import org.hypertrace.core.datamodel.AttributeValue;
import org.junit.jupiter.api.Test;

public class JaegerHTTagsConverterTest {

  @Test
  public void testConvertAttributeToKeyValue() {
    String val = "sample value";
    AttributeValue attributeValue = AttributeValue.newBuilder().setValue(val).build();
    assertEquals(val, JaegerHTTagsConverter.convertAttributeToKeyValue(attributeValue).getVStr());
    assertEquals(
        JaegerSpanInternalModel.ValueType.STRING,
        JaegerHTTagsConverter.convertAttributeToKeyValue(attributeValue).getVType());
  }
}
