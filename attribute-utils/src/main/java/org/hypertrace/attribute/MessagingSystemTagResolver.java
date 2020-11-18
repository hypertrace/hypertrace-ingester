package org.hypertrace.attribute;

import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;

public class MessagingSystemTagResolver {
  private static final String OTEL_MESSAGING_SYSTEM = "messaging.system";
  private static final String OTEL_MESSAGING_URL = "messaging.url";

  // rabbitmq specific attributes
  private static final String OTEL_RABBITMQ_MESSAGING_SYSTEM_VALUE = "rabbitmq";
  private static final String OTEL_RABBITMQ_ROUTING_KEY = "messaging.rabbitmq.routing_key";
  private static final String OTHER_RABBITMQ_ROUTING_KEY = "rabbitmq.routing_key";

  public static Optional<String> getRabbitMQRoutingKey(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, OTHER_RABBITMQ_ROUTING_KEY)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTHER_RABBITMQ_ROUTING_KEY));
    } else if (SpanAttributeUtils.containsAttributeKey(event, OTEL_RABBITMQ_ROUTING_KEY)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, OTEL_RABBITMQ_ROUTING_KEY));
    }
    return Optional.empty();
  }
}
