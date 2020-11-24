package org.hypertrace.semantic.convention.utils.error;

import java.util.Arrays;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Error;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ErrorMetrics;

/**
 * Utility class for fetching error related attributes
 */
public class ErrorSemanticConventionUtils {

  private static final String OTEL_EXCEPTION_TYPE = OTelErrorSemanticConventions.EXCEPTION_TYPE.getValue();
  private static final String OTEL_EXCEPTION_MESSAGE = OTelErrorSemanticConventions.EXCEPTION_MESSAGE.getValue();
  private static final String OTEL_EXCEPTION_STACK_TRACE = OTelErrorSemanticConventions.EXCEPTION_STACKTRACE.getValue();

  private static final String[] EXCEPTION_ATTRIBUTES = {
      RawSpanConstants.getValue(Error.ERROR_ERROR),
      RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_ERROR),
      OTEL_EXCEPTION_TYPE,
      OTEL_EXCEPTION_MESSAGE
  };

  private static final String[] EXCEPTION_STACK_TRACE_ATTRIBUTES = {
      RawSpanConstants.getValue(Error.ERROR_STACK_TRACE),
      OTEL_EXCEPTION_STACK_TRACE
  };

  /**
   * This maps to {@link ErrorMetrics#ERROR_METRICS_ERROR_COUNT} enriched constant
   * @param event object encapsulating span data
   * @return check for error in the span event
   */
  public static boolean checkForError(Event event) {
    return Arrays.stream(EXCEPTION_ATTRIBUTES).sequential()
        .anyMatch(v -> SpanAttributeUtils.containsAttributeKey(event, v));
  }

  /**
   * This maps to {@link ErrorMetrics#ERROR_METRICS_EXCEPTION_COUNT} enriched constant
   * @param event object encapsulating span data
   * @return check for exception in the span event
   */
  public static boolean checkForErrorStackTrace(Event event) {
    return Arrays.stream(EXCEPTION_STACK_TRACE_ATTRIBUTES).sequential()
        .anyMatch(v -> SpanAttributeUtils.containsAttributeKey(event, v));
  }
}
