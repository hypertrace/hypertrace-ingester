package org.hypertrace.core.span.normalizer.constants;

public enum OTelSpanTag {
  OTEL_SPAN_TAG_RPC_SYSTEM("rpc.system"),
  OTEL_SPAN_TAG_RPC_SERVICE("rpc.service"),
  OTEL_SPAN_TAG_RPC_METHOD("rpc.method"),

  OTEL_SPAN_TAG_NET_PEER_IP("net.peer.ip"),
  OTEL_SPAN_TAG_NET_PEER_PORT("net.peer.port"),
  OTEL_SPAN_TAG_NET_PEER_NAME("net.peer.name"),
  OTEL_SPAN_TAG_NET_TRANSPORT("net.transport");
  private final String value;

  OTelSpanTag(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
