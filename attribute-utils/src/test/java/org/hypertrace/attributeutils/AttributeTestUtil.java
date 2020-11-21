package org.hypertrace.attributeutils;

import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;

public class AttributeTestUtil {

  public static AttributeValue buildAttributeValue(String value) {
    return AttributeValue.newBuilder().setValue(value).build();
  }

  public static Attributes buildAttributes(Map<String, AttributeValue> map) {
    return Attributes.newBuilder().setAttributeMap(map).build();
  }
}
