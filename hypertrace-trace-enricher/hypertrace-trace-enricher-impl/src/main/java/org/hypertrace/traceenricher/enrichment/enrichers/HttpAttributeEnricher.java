package org.hypertrace.traceenricher.enrichment.enrichers;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;

import com.google.common.base.Splitter;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Http;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpAttributeEnricher extends AbstractTraceEnricher {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpAttributeEnricher.class);

  private static final String HTTP_REQUEST_PATH_ATTR =
      EnrichedSpanConstants.getValue(Http.HTTP_REQUEST_PATH);
  private static final String HTTP_REQUEST_QUERY_PARAM_ATTR =
      EnrichedSpanConstants.getValue(Http.HTTP_REQUEST_QUERY_PARAM);
  private static final String PARAM_ATTR_FORMAT = "%s.%s";
  private static final String QUERY_PARAM_DELIMITER = "&";
  private static final String QUERY_PARAM_KEY_VALUE_DELIMITER = "=";

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {

    EnrichedSpanUtils.getPath(event)
        .ifPresent(
            path ->
                addEnrichedAttribute(
                    event, HTTP_REQUEST_PATH_ATTR, AttributeValueCreator.create(path)));

    EnrichedSpanUtils.getQueryString(event)
        .ifPresent(
            queryString -> {
              Map<String, List<String>> paramNameToValues =
                  getQueryParamsFromQueryString(queryString, HexUtils.getHex(event.getEventId()));
              for (Map.Entry<String, List<String>> queryParamEntry : paramNameToValues.entrySet()) {
                if (queryParamEntry.getValue().isEmpty()) {
                  continue;
                }
                String queryParamAttr = queryParamEntry.getKey();
                // Getting a single value out of all values(for backward compatibility)
                String queryParamStringValue = queryParamEntry.getValue().get(0);
                AttributeValue attributeValue =
                    fastNewBuilder(AttributeValue.Builder.class)
                        .setValue(queryParamStringValue)
                        .setValueList(queryParamEntry.getValue())
                        .build();
                addEnrichedAttribute(event, queryParamAttr, attributeValue);
              }
            });
  }

  private Map<String, List<String>> getQueryParamsFromQueryString(String queryString, String spanId) {
    return Splitter.on(QUERY_PARAM_DELIMITER).splitToList(queryString).stream()
        // split only on first occurrence of delimiter. eg: cat=1dog=2 should be split to cat ->
        // 1dog=2
        .map(kv -> kv.split(QUERY_PARAM_KEY_VALUE_DELIMITER, 2))
        .filter(kv -> kv.length == 2 && !StringUtils.isEmpty(kv[0]) && !StringUtils.isEmpty(kv[1]))
        .map(
            kv ->
                Pair.of(
                    String.format(
                        PARAM_ATTR_FORMAT, HTTP_REQUEST_QUERY_PARAM_ATTR, decodeParamKey(kv[0], spanId)),
                    decode(kv[1], spanId)))
        .collect(groupingBy(Pair::getKey, mapping(Pair::getValue, toList())));
  }

  private static String decodeParamKey(String input, String spanId) {
    String urlDecodedKey = decode(input, spanId);
    /* '[]' can occur at the end of param name which denotes param can have multiple values,
    we strip '[]' to get original param name */
    if (urlDecodedKey.endsWith("[]") && urlDecodedKey.length() > 2) {
      return urlDecodedKey.substring(0, urlDecodedKey.length() - 2);
    }
    return input;
  }

  private static String decode(String input, String spanId) {
    try {
      return URLDecoder.decode(input, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      LOGGER.error("Cannot decode the input {}, span id {}", input, spanId, e);
      // Falling back to original input if it can't be decoded
      return input;
    }
  }
}
