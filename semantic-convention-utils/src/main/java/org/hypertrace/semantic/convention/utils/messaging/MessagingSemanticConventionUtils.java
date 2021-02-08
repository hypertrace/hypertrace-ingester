package org.hypertrace.semantic.convention.utils.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.semantic.convention.constants.messaging.OtelMessagingSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.RabbitMq;

/**
 * Utility class to fetch messaging system span attributes
 */
public class MessagingSemanticConventionUtils {

  private static final List<String> RABBITMQ_ROUTING_KEYS =
      new ArrayList<>(Arrays.asList(
          RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY),
          OtelMessagingSemanticConventions.RABBITMQ_ROUTING_KEY.getValue()
  ));

  /**
   * @param event Object encapsulating span data
   * @return Routing key for Rabbit mq messaging system
   */
  public static Optional<String> getRabbitMqRoutingKey(Event event) {
    if (!isRabbitMqBackend(event)) {
      return Optional.empty();
    }
    return Optional.ofNullable(SpanAttributeUtils.getFirstAvailableStringAttribute(
        event, RABBITMQ_ROUTING_KEYS));
  }

  public static boolean isRabbitMqBackend(Event event) {
    return SpanAttributeUtils.containsAttributeKey(event, RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY))
        || SpanAttributeUtils.containsAttributeKey(event, OtelMessagingSemanticConventions.RABBITMQ_ROUTING_KEY.getValue());
  }
}
