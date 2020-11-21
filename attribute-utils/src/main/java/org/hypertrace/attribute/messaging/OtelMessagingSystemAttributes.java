package org.hypertrace.attribute.messaging;

/**
 * OTEL specific attributes for Messaging system
 */
public enum OtelMessagingSystemAttributes {
  MESSAGING_SYSTEM("messaging.system"),
  MESSAGING_URL("messaging.url"),
  RABBITMQ_MESSAGING_SYSTEM_VALUE("rabbitmq"),
  RABBITMQ_ROUTING_KEY("rabbitmq.routing_key");

  private final String value;

  OtelMessagingSystemAttributes(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
