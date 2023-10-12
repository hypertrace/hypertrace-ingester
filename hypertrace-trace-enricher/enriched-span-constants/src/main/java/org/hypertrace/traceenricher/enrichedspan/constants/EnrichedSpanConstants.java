package org.hypertrace.traceenricher.enrichedspan.constants;

import com.google.protobuf.ProtocolMessageEnum;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.EnumExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichedSpanConstants {
  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichedSpanConstants.class);

  public static final String SPACE_IDS_ATTRIBUTE = "SPACE_IDS";
  public static final String API_EXIT_CALLS_ATTRIBUTE = "API_EXIT_CALLS";
  public static final String API_CALLEE_NAME_COUNT_ATTRIBUTE = "API_CALLEE_NAME_COUNT";
  public static final String API_TRACE_ERROR_SPAN_COUNT_ATTRIBUTE = "API_TRACE_ERROR_SPAN_COUNT";
  public static final String HEAD_EVENT_ID = "head.event.id";
  public static final String API_EXIT_CALLS_COUNT = "api.exit.calls.count";
  public static final String UNIQUE_API_NODES_COUNT = "unique.apis.count";
  public static final String GRPC_REQUEST_URL = "grpc.request.url";
  public static final String GRPC_REQUEST_ENDPOINT = "grpc.request.endpoint";
  public static final String DROP_TRACE_ATTRIBUTE = "drop.trace";
  public static final String CALLER_SERVICE_ID = "CALLER_SERVICE_ID";
  public static final String CALLER_SERVICE_NAME = "CALLER_SERVICE_NAME";

  /**
   * Returns the constant value for the given Enum.
   *
   * @param key enum key defined in proto files.
   * @return the corresponding string value defined for that enum key.
   */
  public static String getValue(ProtocolMessageEnum key) {
    String value = key.getValueDescriptor().getOptions().getExtension(EnumExtension.stringValue);
    if (value.isEmpty()) {
      LOGGER.error("key {} is not an enriched span constant", key);
    }
    return value;
  }
}
