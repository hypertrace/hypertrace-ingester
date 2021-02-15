package org.hypertrace.core.semantic.convention.constants.messaging;

/**
 * OTEL specific attributes for Messaging system
 */
public enum OtelMessagingSemanticConventions {
  MESSAGING_SYSTEM("messaging.system"),
  MESSAGING_URL("messaging.url"),
  RABBITMQ_MESSAGING_SYSTEM_VALUE("rabbitmq"),
  RABBITMQ_ROUTING_KEY("messaging.rabbitmq.routing_key"),
  KAFKA_MESSAGING_SYSTEM_VALUE("kafka"),
  PRODUCER("PRODUCER"),
  CONSUMER("CONSUMER");

  private final String value;

  OtelMessagingSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
