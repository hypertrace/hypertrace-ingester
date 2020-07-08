package org.hypertrace.traceenricher.enrichment.enrichers;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
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

        Map<String, List<String>> paramNameToValues = getQueryParamValues(url);
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

  private Map<String, List<String>> getQueryParamValues(String url) {
    try {
      List<NameValuePair> queryParams = URLEncodedUtils.parse(new URI(url),
          StandardCharsets.UTF_8);
      Map<String, List<String>> paramNameToValues = new HashMap<>();
      for (NameValuePair queryParam : queryParams) {
        //appending the queryParam name to http.request.query.param prefix
        String queryParamAttr =
            String.format(PARAM_ATTR_FORMAT, HTTP_REQUEST_QUERY_PARAM_ATTR, queryParam.getName());
        if (!paramNameToValues.containsKey(queryParamAttr)) {
          paramNameToValues.put(queryParamAttr, new ArrayList<>());
        }
        paramNameToValues.get(queryParamAttr).add(queryParam.getValue());
      }
      return paramNameToValues;
    } catch (URISyntaxException e) {
      //this exception should never be thrown, since we already
      //checked for the URL validity
      //We just have to add a try/catch block, because URLEncodedUtils need URI as input
      //instead of URL
      return new HashMap<>();
    }
  }
}
