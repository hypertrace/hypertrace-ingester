package org.hypertrace.trace.reader.attributes;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.AttributeValueMap;
import org.hypertrace.entity.data.service.v1.Value;

public class EntityUtil {
  public static Map<String, AttributeValue> buildAttributesWithKeyValue(String key, String value) {
    return buildAttributesWithKeyValues(Map.of(key, value));
  }

  public static Map<String, AttributeValue> buildAttributesWithKeyValues(
      Map<String, String> valueMap) {

    return valueMap.entrySet().stream()
        .map(entry -> Map.entry(entry.getKey(), buildAttributeValue(entry.getValue())))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  public static AttributeValue buildAttributeValue(String value) {
    return AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString(value).build())
        .build();
  }

  public static AttributeValue buildAttributeValueList(List<String> valueList) {
    return AttributeValue.newBuilder()
        .setValueList(
            AttributeValueList.newBuilder()
                .addAllValues(
                    valueList.stream()
                        .map(EntityUtil::buildAttributeValue)
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  public static AttributeValue buildAttributeValueMap(Map<String, String> valueMap) {
    return AttributeValue.newBuilder()
        .setValueMap(
            AttributeValueMap.newBuilder()
                .putAllValues(
                    valueMap.entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey, entry -> buildAttributeValue(entry.getValue()))))
                .build())
        .build();
  }
}
