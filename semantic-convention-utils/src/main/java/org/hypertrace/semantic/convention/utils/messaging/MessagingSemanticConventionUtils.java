package org.hypertrace.semantic.convention.utils.messaging;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.semantic.convention.constants.messaging.OtelMessagingSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OpenTracingSpanSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.RabbitMq;
import org.hypertrace.semantic.convention.utils.db.DbSemanticConventionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to fetch messaging system span attributes */
public class MessagingSemanticConventionUtils {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MessagingSemanticConventionUtils.class);

  private static final String MESSAGING_SYSTEM =
      OtelMessagingSemanticConventions.MESSAGING_SYSTEM.getValue();
  private static final String MESSAGING_URL =
      OtelMessagingSemanticConventions.MESSAGING_URL.getValue();
  private static final String RABBITMQ_SYSTEM_VALUE =
      OtelMessagingSemanticConventions.RABBITMQ_MESSAGING_SYSTEM_VALUE.getValue();
  private static final String KAFKA_SYSTEM_VALUE =
      OtelMessagingSemanticConventions.KAFKA_MESSAGING_SYSTEM_VALUE.getValue();
  private static final String SQS_SYSTEM_VALUE =
      OtelMessagingSemanticConventions.AWS_SQS_MESSAGING_SYSTEM_VALUE.getValue();
  private static final String PEER_SERVICE_NAME =
      OpenTracingSpanSemanticConventions.PEER_NAME.getValue();
  private static final String MESSAGING_OPERATION =
      OtelMessagingSemanticConventions.MESSAGING_OPERATION.getValue();
  private static final String RABBITMQ_COMMAND_VALUE =
      OtelMessagingSemanticConventions.RABBITMQ_COMMAND.getValue();

  private static final List<String> RABBITMQ_ROUTING_KEYS =
      new ArrayList<>(
          Arrays.asList(
              RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY),
              OtelMessagingSemanticConventions.RABBITMQ_ROUTING_KEY.getValue()));

  /**
   * @param event Object encapsulating span data
   * @return Routing key for Rabbit mq messaging system
   */
  public static Optional<String> getRabbitMqRoutingKey(Event event) {
    if (!isRabbitMqBackend(event)) {
      return Optional.empty();
    }
    return Optional.ofNullable(
        SpanAttributeUtils.getFirstAvailableStringAttribute(event, RABBITMQ_ROUTING_KEYS));
  }

  public static boolean isRabbitMqBackend(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, MESSAGING_SYSTEM)) {
      return RABBITMQ_SYSTEM_VALUE.equals(
          SpanAttributeUtils.getStringAttributeWithDefault(
              event, MESSAGING_SYSTEM, StringUtils.EMPTY));
    }

    return SpanAttributeUtils.containsAttributeKey(
            event, RawSpanConstants.getValue(RabbitMq.RABBIT_MQ_ROUTING_KEY))
        || SpanAttributeUtils.containsAttributeKey(
            event, OtelMessagingSemanticConventions.RABBITMQ_ROUTING_KEY.getValue());
  }

  public static Optional<String> getKafkaBackendURI(Event event) {
    if (!isKafkaBackend(event)) {
      return Optional.empty();
    }

    Optional<String> OtBackendURI =
        DbSemanticConventionUtils.getBackendURIForOpenTracingFormat(event);
    if (OtBackendURI.isPresent()) {
      return OtBackendURI;
    }

    if (SpanAttributeUtils.containsAttributeKey(event, MESSAGING_URL)) {
      return Optional.of(SpanAttributeUtils.getStringAttribute(event, MESSAGING_URL));
    }

    return DbSemanticConventionUtils.getBackendURIForOtelFormat(event);
  }

  public static boolean isKafkaBackend(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, PEER_SERVICE_NAME)) {
      return KAFKA_SYSTEM_VALUE.equals(
          SpanAttributeUtils.getStringAttributeWithDefault(
              event, PEER_SERVICE_NAME, StringUtils.EMPTY));
    }

    if (SpanAttributeUtils.containsAttributeKey(event, MESSAGING_SYSTEM)) {
      return KAFKA_SYSTEM_VALUE.equals(
          SpanAttributeUtils.getStringAttributeWithDefault(
              event, MESSAGING_SYSTEM, StringUtils.EMPTY));
    }

    return false;
  }

  public static List<String> getAttributeKeysForMessagingOperation() {
    return Lists.newArrayList(Sets.newHashSet(MESSAGING_OPERATION));
  }

  public static List<String> getAttributeKeysForRabbitmqCommand() {
    return Lists.newArrayList(Sets.newHashSet(RABBITMQ_COMMAND_VALUE));
  }

  public static String getMessagingOperation(Event event) {
    return SpanAttributeUtils.getFirstAvailableStringAttribute(
        event, MessagingSemanticConventionUtils.getAttributeKeysForMessagingOperation());
  }

  public static String getRabbitmqOperation(Event event) {
    String messagingOperation =
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, MessagingSemanticConventionUtils.getAttributeKeysForMessagingOperation());
    String rabbitmqCommand =
        SpanAttributeUtils.getFirstAvailableStringAttribute(
            event, MessagingSemanticConventionUtils.getAttributeKeysForRabbitmqCommand());

    if (messagingOperation != null) {
      return messagingOperation;
    } else return rabbitmqCommand;
  }

  public static Optional<String> getSqsBackendURI(Event event) {
    if (!isSqsBackend(event)) {
      return Optional.empty();
    }

    Optional<String> OtBackendURI =
        DbSemanticConventionUtils.getBackendURIForOpenTracingFormat(event);
    if (OtBackendURI.isPresent()) {
      return OtBackendURI;
    }

    if (SpanAttributeUtils.containsAttributeKey(event, MESSAGING_URL)) {
      return getHostPortFromURL(SpanAttributeUtils.getStringAttribute(event, MESSAGING_URL));
    }

    return DbSemanticConventionUtils.getBackendURIForOtelFormat(event);
  }

  public static Optional<String> getHostPortFromURL(String url) {
    try {
      URL backendURL = new URL(url);
      String host = backendURL.getHost();
      Integer port = backendURL.getPort();
      return Optional.of(String.format("%s:%s", host, port));
    } catch (MalformedURLException e) {
      LOGGER.warn("Unable to construct backendURI from {}", url);
      return Optional.empty();
    }
  }

  public static boolean isSqsBackend(Event event) {
    if (SpanAttributeUtils.containsAttributeKey(event, PEER_SERVICE_NAME)) {
      return SQS_SYSTEM_VALUE.equals(
          SpanAttributeUtils.getStringAttributeWithDefault(
              event, PEER_SERVICE_NAME, StringUtils.EMPTY));
    }

    if (SpanAttributeUtils.containsAttributeKey(event, MESSAGING_SYSTEM)) {
      return SQS_SYSTEM_VALUE.equals(
          SpanAttributeUtils.getStringAttributeWithDefault(
              event, MESSAGING_SYSTEM, StringUtils.EMPTY));
    }
    return false;
  }
}
