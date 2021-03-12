package org.hypertrace.traceenricher.enrichedspan.constants;

import com.google.protobuf.ProtocolMessageEnum;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.EnumExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichedSpanConstants {
  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichedSpanConstants.class);

  public static final String SPACE_IDS_ATTRIBUTE = "SPACE_IDS";
  public static final String API_EXIT_CALLS_ATTRIBUTE = "API_EXIT_CALLS";
  public static final String API_EXIT_SERVICES_ATTRIBUTE = "API_EXIT_SERVICES";

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
