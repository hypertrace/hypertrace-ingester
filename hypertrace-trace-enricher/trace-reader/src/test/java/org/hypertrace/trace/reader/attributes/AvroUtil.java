package org.hypertrace.trace.reader.attributes;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.StructuredTrace;

public class AvroUtil {

  public static StructuredTrace.Builder defaultedStructuredTraceBuilder() {
    return StructuredTrace.newBuilder()
        .setCustomerId("defaultCustomerId")
        .setTraceId(toByteBuffer("defaultTraceId"))
        .setEntityList(Collections.emptyList())
        .setEventList(Collections.emptyList())
        .setEntityEdgeList(Collections.emptyList())
        .setEventEdgeList(Collections.emptyList())
        .setEntityEventEdgeList(Collections.emptyList());
  }

  public static Event.Builder defaultedEventBuilder() {
    return Event.newBuilder()
        .setCustomerId("defaultCustomerId")
        .setEventId(toByteBuffer("defaultEventId"));
  }

  public static Metrics buildMetricsWithKeyValue(String key, double value) {
    return Metrics.newBuilder().setMetricMap(Map.of(key, buildMetricValue(value))).build();
  }

  public static Attributes buildAttributesWithKeyValue(String key, String value) {
    return buildAttributesWithKeyValues(Map.of(key, value));
  }

  public static Attributes buildAttributesWithKeyValues(Map<String, String> valueMap) {
    Map<String, AttributeValue> convertedValueMap =
        valueMap.entrySet().stream()
            .map(entry -> Map.entry(entry.getKey(), buildAttributeValue(entry.getValue())))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    return Attributes.newBuilder().setAttributeMap(convertedValueMap).build();
  }

  public static AttributeValue buildAttributeValue(String value) {
    return AttributeValue.newBuilder().setValue(value).build();
  }

  public static AttributeValue buildAttributeValueList(List<String> valueList) {
    return AttributeValue.newBuilder().setValueList(valueList).build();
  }

  public static AttributeValue buildAttributeValueMap(Map<String, String> valueMap) {
    return AttributeValue.newBuilder().setValueMap(valueMap).build();
  }

  public static MetricValue buildMetricValue(double value) {
    return MetricValue.newBuilder().setValue(value).build();
  }

  public static ByteBuffer toByteBuffer(String value) {
    return ByteBuffer.wrap(value.getBytes());
  }
}
