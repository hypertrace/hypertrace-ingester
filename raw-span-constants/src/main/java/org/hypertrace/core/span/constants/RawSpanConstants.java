package org.hypertrace.core.span.constants;

import com.google.protobuf.ProtocolMessageEnum;
import org.hypertrace.core.span.constants.v1.EnumExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawSpanConstants {
  private static final Logger LOGGER = LoggerFactory.getLogger(RawSpanConstants.class);

  /**
   * Returns the constant value for the given Enum.
   *
   * @param key enum key defined in proto files.
   * @return the corresponding string value defined for that enum key.
   */
  public static String getValue(ProtocolMessageEnum key) {
    String value = key.getValueDescriptor().getOptions().getExtension(EnumExtension.stringValue);
    if (value.isEmpty()) {
      LOGGER.error("key {} is not a raw span constant", key);
    }
    return value;
  }
}
