package org.hypertrace.attribute.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.core.span.constants.v1.OTSpanTag;

public class HttpTagResolver {

  private static final String OTEL_HTTP_METHOD = "http.method";
  private static final String OTHER_HTTP_METHOD = RawSpanConstants.getValue(Http.HTTP_METHOD);
  private static final String OTHER_HTTP_REQUEST_METHOD = RawSpanConstants.getValue(Http.HTTP_REQUEST_METHOD);

  // status code
  private static final String OTEL_HTTP_STATUS_CODE = "http.status_code";
  private static final String[] OTHER_HTTP_STATUS_CODES =
      {
          RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE),
          RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_CODE)
      };

  // status message
  private static final String OTHER_HTTP_RESPONSE_STATUS_MESSAGE = RawSpanConstants.getValue(Http.HTTP_RESPONSE_STATUS_MESSAGE);

  public static List<String> getTagsForHttpMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_HTTP_METHOD, OTEL_HTTP_METHOD));
  }

  public static List<String> getTagsForHttpRequestMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_HTTP_REQUEST_METHOD));
  }

  public static List<String> getHttpStatusCodeKeys() {
    List<String> httpStatusCodeKeys = new ArrayList<>(Arrays.asList(OTHER_HTTP_STATUS_CODES));
    httpStatusCodeKeys.add(OTEL_HTTP_STATUS_CODE);
    return httpStatusCodeKeys;
  }

  public static String getHttpStatusMessage(Event event, String statusCode) {
    String statusMessage = SpanAttributeUtils.getStringAttribute(
        event,
        OTHER_HTTP_RESPONSE_STATUS_MESSAGE);
    if (statusMessage == null) {
      statusMessage = HttpCodeMapper.getMessage(statusCode);
    }
    return statusMessage;
  }
}
