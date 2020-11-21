package org.hypertrace.traceenricher;

import org.hypertrace.core.datamodel.AttributeValue;

public class TestUtil {
  public static AttributeValue buildAttributeValue(String value) {
    return AttributeValue.newBuilder().setValue(value).build();
  }
}
