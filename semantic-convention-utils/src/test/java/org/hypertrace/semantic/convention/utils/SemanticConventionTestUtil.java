package org.hypertrace.semantic.convention.utils;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;

import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;

public class SemanticConventionTestUtil {

  public static AttributeValue buildAttributeValue(String value) {
    return AttributeValue.newBuilder().setValue(value).build();
  }

  public static Attributes buildAttributes(Map<String, AttributeValue> map) {
    return fastNewBuilder(Attributes.Builder.class).setAttributeMap(map).build();
  }
}
