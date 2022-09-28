package org.hypertrace.traceenricher.util;

import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.Resource;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.SpanNamePrefix;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.entity.data.service.v1.Value;

public class EnricherUtil {

  public static void setAttributeIfExist(Event event, Builder entityBuilder, String attributeKey) {
    if (event.getAttributes().getAttributeMap().get(attributeKey) != null) {
      entityBuilder.putAttributes(
          attributeKey,
          createAttributeValue(
              event.getAttributes().getAttributeMap().get(attributeKey).getValue()));
    }
  }

  public static Map<String, AttributeValue> getAttributesForFirstExistingKey(
      Event event, List<String> attributeKeys) {
    Map<String, AttributeValue> attributes = new HashMap<>();
    for (String attributeKey : attributeKeys) {
      if (event.getAttributes().getAttributeMap().get(attributeKey) != null) {
        attributes.put(
            attributeKey,
            createAttributeValue(
                event.getAttributes().getAttributeMap().get(attributeKey).getValue()));
      }
    }

    return Collections.unmodifiableMap(attributes);
  }

  public static Optional<org.hypertrace.core.datamodel.AttributeValue> getAttribute(
      Attributes attributes, String key) {
    return Optional.ofNullable(attributes)
        .map(Attributes::getAttributeMap)
        .map(attributeMap -> attributeMap.get(key));
  }

  public static Optional<org.hypertrace.core.datamodel.AttributeValue> getResourceAttribute(
      StructuredTrace trace, Event span, String key) {
    if (span.getResourceIndex() < 0 || span.getResourceIndex() >= trace.getResourceList().size()) {
      return Optional.empty();
    }

    return Optional.of(trace.getResourceList().get(span.getResourceIndex()))
        .map(Resource::getAttributes)
        .flatMap(attributes -> getAttribute(attributes, key));
  }

  public static void setAttributeForFirstExistingKey(
      Event event, Builder entityBuilder, List<String> attributeKeys) {
    for (String attributeKey : attributeKeys) {
      if (event.getAttributes().getAttributeMap().get(attributeKey) != null) {
        entityBuilder.putAttributes(
            attributeKey,
            createAttributeValue(
                event.getAttributes().getAttributeMap().get(attributeKey).getValue()));
        return;
      }
    }
  }

  public static org.hypertrace.entity.data.service.v1.AttributeValue createAttributeValue(
      String value) {
    return AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString(value).build())
        .build();
  }

  private static org.hypertrace.core.datamodel.AttributeValue createAttributeValue(
      AttributeValue attr) {
    return AttributeValueCreator.create(attr.getValue().getString());
  }

  public static boolean isSentGrpcEvent(Event event) {
    return StringUtils.startsWith(
        event.getEventName(), RawSpanConstants.getValue(SpanNamePrefix.SPAN_NAME_PREFIX_SENT));
  }

  public static boolean isReceivedGrpcEvent(Event event) {
    return StringUtils.startsWith(
        event.getEventName(), RawSpanConstants.getValue(SpanNamePrefix.SPAN_NAME_PREFIX_RECV));
  }
}
