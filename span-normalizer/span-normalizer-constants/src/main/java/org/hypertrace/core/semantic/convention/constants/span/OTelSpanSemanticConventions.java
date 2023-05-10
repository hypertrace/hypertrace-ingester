package org.hypertrace.core.semantic.convention.constants.span;

/** Otel attributes for span */
public enum OTelSpanSemanticConventions {
  SPAN_KIND("span_kind"),
  SPAN_KIND_SERVER_VALUE("server"),
  SPAN_KIND_CLIENT_VALUE("client"),
  NET_PEER_IP("net.peer.ip"),
  NET_PEER_PORT("net.peer.port"),
  NET_PEER_NAME("net.peer.name"),
  NET_SOCK_PEER_ADDR("net.sock.peer.addr"),
  NET_TRANSPORT("net.transport"),
  HTTP_CLIENT_IP("http.client_ip");

  private final String value;

  OTelSpanSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
