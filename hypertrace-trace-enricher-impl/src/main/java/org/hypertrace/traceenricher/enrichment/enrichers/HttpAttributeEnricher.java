package org.hypertrace.traceenricher.enrichment.enrichers;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Http;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpAttributeEnricher extends AbstractTraceEnricher {
  private static Logger LOG = LoggerFactory.getLogger(HttpAttributeEnricher.class);
  private final static String HTTP_REQUEST_PATH_ATTR =
      EnrichedSpanConstants.getValue(Http.HTTP_REQUEST_PATH);
  private final static String HTTP_REQUEST_QUERY_PARAM_ATTR =
      EnrichedSpanConstants.getValue(Http.HTTP_REQUEST_QUERY_PARAM);
  private final static String PARAM_ATTR_FORMAT = "%s.%s";
  private final static String QUERY_PARAM_DELIMITER = "&";
  private final static String QUERY_PARAM_KEY_VALUE_DELIMITER = "=";

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    String url = EnrichedSpanUtils.getFullHttpUrl(event).orElse(null);
    if (url != null) {
      URL fullUrl = null;
      try {
        fullUrl = new URL(url);
      } catch (MalformedURLException e) {
        LOG.warn("The url {} is not a valid format url", url);
      }

      if (fullUrl != null) {
        String path = fullUrl.getPath();
        addEnrichedAttribute(event, HTTP_REQUEST_PATH_ATTR, AttributeValueCreator.create(path));

        Map<String, List<String>> paramNameToValues = getQueryParamsFromUrl(fullUrl);
        for (Map.Entry<String, List<String>> queryParamEntry : paramNameToValues.entrySet()) {
          if (queryParamEntry.getValue().isEmpty()) {
            continue;
          }
          String queryParamAttr = queryParamEntry.getKey();
          //Getting a single value out of all values(for backward compatibility)
          String queryParamStringValue = queryParamEntry.getValue().get(0);
          AttributeValue attributeValue = AttributeValue.newBuilder()
              .setValue(queryParamStringValue)
              .setValueList(queryParamEntry.getValue())
              .build();
          addEnrichedAttribute(event, queryParamAttr, attributeValue);
        }
      }
    }
  }

  private Map<String, List<String>> getQueryParamsFromUrl(URL url) {
    Map<String, List<String>> queryParamNameToValues = new HashMap<>();
    if(StringUtils.isEmpty(url.getQuery())) {
      return queryParamNameToValues;
    }
    String[] keyValuePairStrings = url.getQuery().split(QUERY_PARAM_DELIMITER);
    for (String pair : keyValuePairStrings) {
      int idx = pair.indexOf(QUERY_PARAM_KEY_VALUE_DELIMITER);
      String key = idx > 0 ?
          URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8) : pair;
      String attributeKey =
          String.format(PARAM_ATTR_FORMAT, HTTP_REQUEST_QUERY_PARAM_ATTR, key);
      if (!queryParamNameToValues.containsKey(attributeKey)) {
        queryParamNameToValues.put(attributeKey, new ArrayList<>());
      }
      String value = idx > 0 && pair.length() > idx + 1 ?
          URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8) : "";
      queryParamNameToValues.get(attributeKey).add(value);
    }
    return queryParamNameToValues;
  }
}
