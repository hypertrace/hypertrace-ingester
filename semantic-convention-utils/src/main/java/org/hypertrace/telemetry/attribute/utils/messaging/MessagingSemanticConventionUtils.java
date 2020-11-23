package org.hypertrace.telemetry.attribute.utils.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.RabbitMq;

/**
 * Utility class to fetch messaging system span attributes
 */
public class MessagingSemanticConventionUtils {

  private static final List<String> RABBITMQ_ROUTING_KEYS =
      new ArrayList<>(Arrays.asList(
          RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY),
          OtelMessagingAttributes.RABBITMQ_ROUTING_KEY.getValue()
  ));

  /**
   * @param event Object encapsulating span data
   * @return Routing key for Rabbit mq messaging system
   */
  public static Optional<String> getRabbitMqRoutingKey(Event event) {
    return Optional.ofNullable(SpanAttributeUtils.getFirstAvailableStringAttribute(
        event, RABBITMQ_ROUTING_KEYS));
  }
}
