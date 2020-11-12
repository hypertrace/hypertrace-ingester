package org.hypertrace.trace.reader.entities;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.AttributeValueMap;
import org.hypertrace.entity.data.service.v1.Value;

public class AttributeValueUtil {

  public static AttributeValue stringAttributeValue(String stringValue) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setString(stringValue)).build();
  }

  public static AttributeValue stringListAttributeValue(String... stringValues) {
    List<AttributeValue> values =
        Arrays.stream(stringValues)
            .map(AttributeValueUtil::stringAttributeValue)
            .collect(Collectors.toList());
    return AttributeValue.newBuilder()
        .setValueList(AttributeValueList.newBuilder().addAllValues(values))
        .build();
  }

  public static AttributeValue stringMapAttributeValue(Map<String, String> values) {
    Map<String, AttributeValue> convertedValueMap =
        values.entrySet().stream()
            .map(entry -> Map.entry(entry.getKey(), stringAttributeValue(entry.getValue())))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    return AttributeValue.newBuilder()
        .setValueMap(AttributeValueMap.newBuilder().putAllValues(convertedValueMap))
        .build();
  }

  public static AttributeValue longAttributeValue(long longValue) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setLong(longValue)).build();
  }

  public static AttributeValue longListAttributeValue(Long... longValues) {
    List<AttributeValue> values =
        Arrays.stream(longValues)
            .map(AttributeValueUtil::longAttributeValue)
            .collect(Collectors.toList());
    return AttributeValue.newBuilder()
        .setValueList(AttributeValueList.newBuilder().addAllValues(values))
        .build();
  }

  public static AttributeValue doubleAttributeValue(double doubleValue) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setDouble(doubleValue)).build();
  }

  public static AttributeValue doubleListAttributeValue(Double... doubleValues) {
    List<AttributeValue> values =
        Arrays.stream(doubleValues)
            .map(AttributeValueUtil::doubleAttributeValue)
            .collect(Collectors.toList());
    return AttributeValue.newBuilder()
        .setValueList(AttributeValueList.newBuilder().addAllValues(values))
        .build();
  }

  public static AttributeValue booleanAttributeValue(boolean booleanValue) {
    return AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setBoolean(booleanValue))
        .build();
  }

  public static AttributeValue booleanListAttributeValue(Boolean... booleanValues) {
    List<AttributeValue> values =
        Arrays.stream(booleanValues)
            .map(AttributeValueUtil::booleanAttributeValue)
            .collect(Collectors.toList());
    return AttributeValue.newBuilder()
        .setValueList(AttributeValueList.newBuilder().addAllValues(values))
        .build();
  }
}
