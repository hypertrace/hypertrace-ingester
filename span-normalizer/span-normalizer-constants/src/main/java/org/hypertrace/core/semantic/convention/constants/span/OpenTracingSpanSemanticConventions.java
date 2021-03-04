package org.hypertrace.core.semantic.convention.constants.span;

/**
 * Otel attributes for span
 */
public enum OpenTracingSpanSemanticConventions {
  SPAN_KIND("span_kind"),
  SPAN_KIND_SERVER_VALUE("server"),
  SPAN_KIND_CLIENT_VALUE("client"),
  SPAN_KIND_PRODUCER_VALUE("producer"),
  SPAN_KIND_CONSUMER_VALUE("consumer"),
  PEER_IPV4("peer.ipv4"),
  PEER_IPV6("peer.ipv6"),
  PEER_HOSTNAME("peer.hostname"),
  PEER_PORT("peer.port"),
  PEER_NAME("peer.service"),
  PEER_ADDRESS("peer.address");

  private final String value;

  OpenTracingSpanSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
