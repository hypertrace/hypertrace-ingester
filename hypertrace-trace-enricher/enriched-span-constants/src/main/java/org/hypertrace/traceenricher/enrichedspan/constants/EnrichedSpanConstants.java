package org.hypertrace.traceenricher.enrichedspan.constants;

import com.google.protobuf.ProtocolMessageEnum;
import io.micrometer.core.instrument.util.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.EnumExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.*;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.*;


public class EnrichedSpanConstants {
  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichedSpanConstants.class);

  public static final String SPACE_IDS_ATTRIBUTE = "SPACE_IDS";
  public static final String API_EXIT_CALLS_ATTRIBUTE = "API_EXIT_CALLS";
  public static final String API_CALLEE_NAME_COUNT_ATTRIBUTE = "API_CALLEE_NAME_COUNT";
  public static final String API_TRACE_ERROR_SPAN_COUNT_ATTRIBUTE = "API_TRACE_ERROR_SPAN_COUNT";
  private static final String SLASH = "/";

  private static final List<String> METHOD_ATTRIBUTES =
          List.of(
                  RawSpanConstants.getValue(HTTP_REQUEST_METHOD),
                  RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD),
                  OTelHttpSemanticConventions.HTTP_METHOD.getValue());

  private static final List<String> FULL_URL_ATTRIBUTES =
          List.of(
                  RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL),
                  RawSpanConstants.getValue(HTTP_REQUEST_URL),
                  RawSpanConstants.getValue(HTTP_URL),
                  OTelHttpSemanticConventions.HTTP_URL.getValue());

  private static final List<String> URL_PATH_ATTRIBUTES =
          List.of(
                  RawSpanConstants.getValue(HTTP_REQUEST_PATH),
                  RawSpanConstants.getValue(HTTP_PATH),
                  OTelHttpSemanticConventions.HTTP_TARGET.getValue());

  private static final List<String> QUERY_STRING_ATTRIBUTES =
          List.of(
                  RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING)
          );

  private static final List<String> RESPONSE_SIZE_ATTRIBUTES =
          List.of(
                  RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE),
                  RawSpanConstants.getValue(HTTP_RESPONSE_SIZE),
                  OTelHttpSemanticConventions.HTTP_RESPONSE_SIZE.getValue());

  private static final List<String> REQUEST_SIZE_ATTRIBUTES =
          List.of(
                  RawSpanConstants.getValue(ENVOY_REQUEST_SIZE),
                  RawSpanConstants.getValue(HTTP_REQUEST_SIZE),
                  OTelHttpSemanticConventions.HTTP_REQUEST_SIZE.getValue());

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

  public static Optional<String> getHttpMethod(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for(String method: METHOD_ATTRIBUTES){
      if((attributeValueMap.get(method) != null) && ("" != attributeValueMap.get(method).getValue())){
        return Optional.of(attributeValueMap.get(method).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpUrl(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for(String url: FULL_URL_ATTRIBUTES){
      if((attributeValueMap.get(url) != null) && ("" != attributeValueMap.get(url).getValue())){
        return Optional.of(attributeValueMap.get(url).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpPath(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for(String path: URL_PATH_ATTRIBUTES) {
      if(attributeValueMap.get(path)!=null){
        String s = attributeValueMap.get(path).getValue();
        if(StringUtils.isNotBlank(s) && s.startsWith(SLASH)){
         return Optional.of(s);
        }
      }
    }
    return Optional.empty();
  }

  public static Optional<String> getHttpQueryString(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for(String query_string: QUERY_STRING_ATTRIBUTES){
      if((attributeValueMap.get(query_string) != null) && ("" != attributeValueMap.get(query_string).getValue())){
        return Optional.of(attributeValueMap.get(query_string).getValue());
      }
    }
    return Optional.empty();
  }

  public static Optional<Integer> getHttpResponseSize(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for(String rsa: RESPONSE_SIZE_ATTRIBUTES){
      if((attributeValueMap.get(rsa) != null) && ("" != attributeValueMap.get(rsa).getValue())){
        return Optional.of(Integer.parseInt(attributeValueMap.get(rsa).getValue()));
      }
    }
    return Optional.empty();
  }

  public static Optional<Integer> getHttpRequestSize(Event event) {
    Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
    for(String reqsa: REQUEST_SIZE_ATTRIBUTES){
      if((attributeValueMap.get(reqsa) != null) && ("" != attributeValueMap.get(reqsa).getValue())){
        return Optional.of(Integer.parseInt(attributeValueMap.get(reqsa).getValue()));
      }
    }
    return Optional.empty();
  }
}
