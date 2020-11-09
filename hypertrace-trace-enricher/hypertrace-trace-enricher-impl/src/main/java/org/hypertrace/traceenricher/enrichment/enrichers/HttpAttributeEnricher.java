package org.hypertrace.traceenricher.enrichment.enrichers;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.google.common.base.Splitter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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
    if (StringUtils.isEmpty(url.getQuery())) {
      return Collections.emptyMap();
    }
    return Splitter.on(QUERY_PARAM_DELIMITER)
        .splitToList(url.getQuery())
        .stream()
        //split only on first occurrence of delimiter. eg: cat=1dog=2 should be split to cat -> 1dog=2
        .map(kv -> kv.split(QUERY_PARAM_KEY_VALUE_DELIMITER, 2))
        .map(kv -> Pair.of(
            String.format(PARAM_ATTR_FORMAT, HTTP_REQUEST_QUERY_PARAM_ATTR, decodeParamKey(kv[0])),
            kv.length == 2 ? decode(kv[1]) : ""
        ))
        .collect(groupingBy(Pair::getKey, mapping(Pair::getValue, toList())));
  }

  private String decodeParamKey(String input) {
    String urlDecodedKey = decode(input);
    /* '[]' can occur at the end of param name which denotes param can have multiple values,
    we strip '[]' to get original param name */
    if (urlDecodedKey.endsWith("[]") && urlDecodedKey.length() > 2) {
      return urlDecodedKey.substring(0, urlDecodedKey.length() - 2);
    }
    return input;
  }

  private String decode(String input) {
    try {
      return URLDecoder.decode(input, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      LOG.error("Cannot decode the input {}", input, e);
      //Falling back to original input if it can't be decoded
      return input;
    }
  }
}
