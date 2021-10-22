package org.hypertrace.core.span.normalizer.constants;

public enum RpcSpanTag {
  RPC_STATUS_CODE("rpc.status_code"),
  RPC_ERROR_NAME("rpc.error_name"),
  RPC_ERROR_MESSAGE("rpc.error_message"),
  RPC_REQUEST_METADATA("rpc.request.metadata"),
  RPC_RESPONSE_METADATA("rpc.response.metadata"),
  RPC_REQUEST_BODY("rpc.request.body"),
  RPC_RESPONSE_BODY("rpc.response.body"),
  RPC_REQUEST_METADATA_X_FORWARDED_FOR("rpc.request.metadata.x-forwarded-for"),
  RPC_REQUEST_METADATA_AUTHORITY("rpc.request.metadata.:authority"),
  RPC_REQUEST_METADATA_CONTENT_TYPE("rpc.request.metadata.content-type"),
  RPC_REQUEST_METADATA_CONTENT_LENGTH("rpc.request.metadata.content-length"),
  RPC_REQUEST_METADATA_PATH("rpc.request.metadata.:path"),
  RPC_REQUEST_METADATA_USER_AGENT("rpc.request.metadata.user-agent"),
  RPC_RESPONSE_METADATA_CONTENT_TYPE("rpc.response.metadata.content-type"),
  RPC_RESPONSE_METADATA_CONTENT_LENGTH("rpc.response.metadata.content-length"),
  RPC_BODY_DECODE_RAW("rpc.body.decode_raw");

  private final String value;

  RpcSpanTag(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
