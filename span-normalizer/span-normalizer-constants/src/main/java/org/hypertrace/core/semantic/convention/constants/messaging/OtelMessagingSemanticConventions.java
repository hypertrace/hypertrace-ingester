package org.hypertrace.core.semantic.convention.constants.messaging;

/** OTEL specific attributes for Messaging system */
public enum OtelMessagingSemanticConventions {
  MESSAGING_SYSTEM("messaging.system"),
  MESSAGING_URL("messaging.url"),
  RABBITMQ_MESSAGING_SYSTEM_VALUE("rabbitmq"),
  RABBITMQ_ROUTING_KEY("messaging.rabbitmq.routing_key"),
  RABBITMQ_COMMAND("rabbitmq.command"),
  KAFKA_MESSAGING_SYSTEM_VALUE("kafka"),
  AWS_SQS_MESSAGING_SYSTEM_VALUE("sqs"),
  PRODUCER("PRODUCER"),
  CONSUMER("CONSUMER"),
  MESSAGING_OPERATION("messaging.operation"),
  MESSAGING_DESTINATION("messaging.destination"),
  KAFKA_MESSAGING_CONSUMER_GROUP("messaging.kafka.consumer_group");

  private final String value;

  OtelMessagingSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
