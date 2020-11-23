package org.hypertrace.telemetry.attribute.utils.span;

/**
 * Otel attributes for span
 */
public enum OTelSpanAttributes {
  SPAN_KIND("span_kind"),
  SPAN_KIND_SERVER_VALUE("server"),
  SPAN_KIND_CLIENT_VALUE("client"),
  NET_PEER_IP("net.peer.ip"),
  NET_PEER_PORT("net.peer.port"),
  NET_PEER_NAME("net.peer.name"),
  NET_TRANSPORT("net.transport");

  private final String value;

  OTelSpanAttributes(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
