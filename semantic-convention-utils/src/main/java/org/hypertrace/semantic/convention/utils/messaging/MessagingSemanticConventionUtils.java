package org.hypertrace.semantic.convention.utils.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.Opt;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.v1.SpanAttribute;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.hypertrace.core.semantic.convention.constants.messaging.OtelMessagingSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.RabbitMq;

/**
 * Utility class to fetch messaging system span attributes
 */
public class MessagingSemanticConventionUtils {

  private static final String MESSAGING_SYSTEM = OtelMessagingSemanticConventions.MESSAGING_SYSTEM.getValue();
  private static final String MESSAGING_URL = OtelMessagingSemanticConventions.MESSAGING_URL.getValue();
  private static final String KAFKA_SYSTEM_VALUE = OtelMessagingSemanticConventions.KAFKA_MESSAGING_SYSTEM_VALUE.getValue();
  private static final String SQS_SYSTEM_VALUE = OtelMessagingSemanticConventions.AWS_SQS_MESSAGING_SYSTEM_VALUE.getValue();

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

  public static Optional<String> getKafkaBackendURI(Event event) {
    if (!isKafkaBackend(event)) {
      return Optional.empty();
    }

    if(SpanAttributeUtils.containsAttributeKey(event, MESSAGING_URL)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, MESSAGING_URL));
    }
    return DbSemanticConventionUtils.getBackendURIForOtelFormat(event);
  }

  public static boolean isKafkaBackend(Event event) {
    if(SpanAttributeUtils.containsAttributeKey(event, MESSAGING_SYSTEM)) {
      return KAFKA_SYSTEM_VALUE.equals(SpanAttributeUtils.getStringAttributeWithDefault(
          event, MESSAGING_SYSTEM, StringUtils.EMPTY));
    }
    return false;
  }

  public static Optional<String> getSqsBackendURI(Event event) {
    if (!isSqsBackend(event)) {
      return Optional.empty();
    }

    if(SpanAttributeUtils.containsAttributeKey(event, MESSAGING_URL)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, MESSAGING_URL));
    }

    return DbSemanticConventionUtils.getBackendURIForOtelFormat(event);
  }

  public static boolean isSqsBackend(Event event) {
    if(SpanAttributeUtils.containsAttributeKey(event, MESSAGING_SYSTEM)) {
      return SQS_SYSTEM_VALUE.equals(SpanAttributeUtils.getStringAttributeWithDefault(
          event, MESSAGING_SYSTEM, StringUtils.EMPTY));
    }
    return false;
  }
}
