package org.hypertrace.traceenricher.util;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.SpanNamePrefix;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity.Builder;
import org.hypertrace.entity.data.service.v1.Value;

public class EnricherUtil {

  public static void setAttributeIfExist(Event event, Builder entityBuilder, String attributeKey) {
    if (event.getAttributes().getAttributeMap().get(attributeKey) != null) {
      entityBuilder.putAttributes(attributeKey,
          createAttributeValue(event.getAttributes().getAttributeMap().get(attributeKey).getValue()));
    }
  }

  public static void setAttributeForFirstExistingKey(Event event, Builder entityBuilder, List<String> attributeKeys) {
    for (String attributeKey : attributeKeys) {
      if (event.getAttributes().getAttributeMap().get(attributeKey) != null) {
        entityBuilder.putAttributes(attributeKey,
            createAttributeValue(event.getAttributes().getAttributeMap().get(attributeKey).getValue()));
        return;
      }
    }
  }

  public static org.hypertrace.entity.data.service.v1.AttributeValue createAttributeValue(String value) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setString(value).build())
        .build();
  }

  private static org.hypertrace.core.datamodel.AttributeValue createAttributeValue(AttributeValue attr) {
    return AttributeValueCreator.create(attr.getValue().getString());
  }

  public static boolean isSentGrpcEvent(Event event) {
    return StringUtils.startsWith(event.getEventName(),
        RawSpanConstants.getValue(SpanNamePrefix.SPAN_NAME_PREFIX_SENT));
  }

  public static boolean isReceivedGrpcEvent(Event event) {
    return StringUtils.startsWith(event.getEventName(),
        RawSpanConstants.getValue(SpanNamePrefix.SPAN_NAME_PREFIX_RECV));
  }
}
