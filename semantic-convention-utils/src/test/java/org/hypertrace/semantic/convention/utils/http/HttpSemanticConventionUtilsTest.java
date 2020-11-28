package org.hypertrace.semantic.convention.utils.http;

import static org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil.buildAttributeValue;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_HOST;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_NET_HOST_NAME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_NET_HOST_PORT;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_SCHEME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_SERVER_NAME;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_TARGET;
import static org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions.HTTP_URL;
import static org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions.SPAN_KIND;
import static org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions.SPAN_KIND_CLIENT_VALUE;
import static org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions.SPAN_KIND_SERVER_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Maps;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link HttpSemanticConventionUtils}
 */
public class HttpSemanticConventionUtilsTest {

  @Test
  public void testGetHttpUrlForOtelFormat() {
    // http url present
    Map<String, AttributeValue> map = Maps.newHashMap();
    map.put(HTTP_URL.getValue(), buildAttributeValue("https://172.0.8.11:1211/webshop/articles/4?s=1"));
    String url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://172.0.8.11:1211/webshop/articles/4?s=1", url);

    // host & target present
    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(HTTP_HOST.getValue(), buildAttributeValue("example.com:1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://example.com:1211/webshop/articles/4?s=1", url);

    // client span
    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(OTelSpanSemanticConventions.NET_PEER_NAME.getValue(), buildAttributeValue("example.com"));
    map.put(OTelSpanSemanticConventions.NET_PEER_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(SPAN_KIND.getValue(), buildAttributeValue(SPAN_KIND_CLIENT_VALUE.getValue()));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://example.com:1211/webshop/articles/4?s=1", url);

    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(OTelSpanSemanticConventions.NET_PEER_IP.getValue(), buildAttributeValue("172.0.8.11"));
    map.put(OTelSpanSemanticConventions.NET_PEER_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(SPAN_KIND.getValue(), buildAttributeValue(SPAN_KIND_CLIENT_VALUE.getValue()));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://172.0.8.11:1211/webshop/articles/4?s=1", url);

    // server span
    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(HTTP_SERVER_NAME.getValue(), buildAttributeValue("example.com"));
    map.put(HTTP_NET_HOST_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(SPAN_KIND.getValue(), buildAttributeValue(SPAN_KIND_SERVER_VALUE.getValue()));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://example.com:1211/webshop/articles/4?s=1", url);

    map.clear();
    map.put(HTTP_SCHEME.getValue(), buildAttributeValue("https"));
    map.put(HTTP_NET_HOST_NAME.getValue(), buildAttributeValue("example.com"));
    map.put(HTTP_NET_HOST_PORT.getValue(), buildAttributeValue("1211"));
    map.put(HTTP_TARGET.getValue(), buildAttributeValue("/webshop/articles/4?s=1"));
    map.put(SPAN_KIND.getValue(), buildAttributeValue(SPAN_KIND_SERVER_VALUE.getValue()));
    url = HttpSemanticConventionUtils.getHttpUrlForOTelFormat(map).get();
    assertEquals("https://example.com:1211/webshop/articles/4?s=1", url);
  }
}
