package org.hypertrace.semantic.convention.utils.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.core.span.constants.v1.OTSpanTag;

/**
 * Utility class to fetch http span attributes
 */
public class HttpSemanticConventionUtils {

  // otel specific attributes
  private static final String OTEL_HTTP_METHOD = OTelHttpSemanticConventions.HTTP_METHOD.getValue();
  private static final String OTEL_HTTP_STATUS_CODE = OTelHttpSemanticConventions.HTTP_STATUS_CODE.getValue();

  private static final String OTHER_HTTP_METHOD = RawSpanConstants.getValue(Http.HTTP_METHOD);
  private static final String OTHER_HTTP_REQUEST_METHOD = RawSpanConstants.getValue(Http.HTTP_REQUEST_METHOD);
  private static final String[] OTHER_HTTP_STATUS_CODES =
      {
          RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE),
          RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_CODE)
      };
  private static final String OTHER_HTTP_RESPONSE_STATUS_MESSAGE =
      RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_MESSAGE);

  /**
   * @return attribute keys for http method
   */
  public static List<String> getAttributeKeysForHttpMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_HTTP_METHOD, OTEL_HTTP_METHOD));
  }

  /**
   * @return attribute keys for http request method
   */
  public static List<String> getAttributeKeysForHttpRequestMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_HTTP_REQUEST_METHOD));
  }
}
