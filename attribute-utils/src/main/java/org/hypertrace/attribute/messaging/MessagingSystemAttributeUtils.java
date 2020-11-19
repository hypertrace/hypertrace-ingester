package org.hypertrace.attribute.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.RabbitMq;

public class MessagingSystemAttributeUtils {

  private static final List<String> RABBITMQ_ROUTING_KEYS =
      new ArrayList<>(Arrays.asList(
          OtelMessagingSystemAttributes.RABBITMQ_ROUTING_KEY.getValue(),
          RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY)
  ));

  public static Optional<String> getRabbitMqRoutingKey(Event event) {
    return Optional.ofNullable(SpanAttributeUtils.getFirstAvailableStringAttribute(
        event, RABBITMQ_ROUTING_KEYS));
  }
}
