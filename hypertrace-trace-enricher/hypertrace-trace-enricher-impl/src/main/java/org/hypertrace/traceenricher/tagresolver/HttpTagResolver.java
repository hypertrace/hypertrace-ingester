package org.hypertrace.traceenricher.tagresolver;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Http;

public class HttpTagResolver {

  private static final String OTEL_HTTP_METHOD = "http.method";
  private static final String OTHER_HTTP_METHOD = RawSpanConstants.getValue(Http.HTTP_METHOD);
  private static final String OTHER_HTTP_REQUEST_METHOD = RawSpanConstants.getValue(Http.HTTP_REQUEST_METHOD);

  public static List<String> getTagsForHttpMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_HTTP_METHOD, OTEL_HTTP_METHOD));
  }

  public static List<String> getTagsForHttpRequestMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTHER_HTTP_REQUEST_METHOD));
  }
}
