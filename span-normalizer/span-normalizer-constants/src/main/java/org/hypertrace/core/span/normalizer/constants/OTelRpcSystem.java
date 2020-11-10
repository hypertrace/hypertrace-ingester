package org.hypertrace.core.span.normalizer.constants;

public enum OTelRpcSystem {
  OTEL_RPC_SYSTEM_GRPC("grpc"),
  OTEL_RPC_SYSTEM_JAVA_RMI("java_rmi"),
  OTEL_RPC_SYSTEM_WCF("wcf");

  private final String value;

  OTelRpcSystem(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
