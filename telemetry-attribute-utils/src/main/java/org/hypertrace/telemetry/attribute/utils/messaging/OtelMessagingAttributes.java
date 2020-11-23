package org.hypertrace.telemetry.attribute.utils.messaging;

/**
 * OTEL specific attributes for Messaging system
 */
public enum OtelMessagingAttributes {
  MESSAGING_SYSTEM("messaging.system"),
  MESSAGING_URL("messaging.url"),
  RABBITMQ_MESSAGING_SYSTEM_VALUE("rabbitmq"),
  RABBITMQ_ROUTING_KEY("messaging.rabbitmq.routing_key");

  private final String value;

  OtelMessagingAttributes(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
