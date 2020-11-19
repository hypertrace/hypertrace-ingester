package org.hypertrace.attribute.exceptions;

import java.util.Arrays;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Error;
import org.hypertrace.core.span.constants.v1.OTSpanTag;

public class ExceptionTagResolver {

  private static final String OTEL_EXCEPTION_TYPE = "exception.type";
  private static final String OTEL_EXCEPTION_MESSAGE = "exception.message";

  private static final String[] EXCEPTIONS_TAGS = {
      RawSpanConstants.getValue(Error.ERROR_ERROR),
      RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_ERROR),
      OTEL_EXCEPTION_TYPE
  };

  public static boolean checkForError(Map<String, AttributeValue> attributeFieldMap) {
    return Arrays.stream(EXCEPTIONS_TAGS).sequential().anyMatch(attributeFieldMap::containsKey);
  }

}
