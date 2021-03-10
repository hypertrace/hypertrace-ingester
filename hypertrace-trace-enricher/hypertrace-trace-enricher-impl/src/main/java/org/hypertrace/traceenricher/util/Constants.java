package org.hypertrace.traceenricher.util;

import com.google.protobuf.ProtocolMessageEnum;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;

/** Convenience class to easily read attribute names with short method calls. */
public class Constants {
  public static String getEntityConstant(ProtocolMessageEnum attr) {
    return EntityConstants.getValue(attr);
  }

  public static String getEnrichedSpanConstant(ProtocolMessageEnum attr) {
    return EnrichedSpanConstants.getValue(attr);
  }

  public static String getRawSpanConstant(ProtocolMessageEnum attr) {
    return RawSpanConstants.getValue(attr);
  }
}
