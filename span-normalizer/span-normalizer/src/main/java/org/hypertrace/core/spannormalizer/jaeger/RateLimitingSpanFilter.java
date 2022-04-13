package org.hypertrace.core.spannormalizer.jaeger;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.datamodel.Event;

// make uuid from tenantId and groupingKey
// add config in helm
public class RateLimitingSpanFilter {

  private static final String RATE_LIMIT_CONFIG_PATH = "rate.limit.config";
  private static final String TENANT_ID_KEY = "tenantId";
  private static final String GROUPING_KEY_KEY = "groupingKey";
  private static final String MAX_SPANS_PER_MINUTE_KEY = "maxSpansPerMinute";
  private static final long SPAN_COUNT_WINDOW = 3600; // fixed at 1minute

  Map<String, Map<Long, Long>> tenantSpanCountMap = new HashMap<>();
  Map<String, Long> tenantMaxSpansPerMinuteMap = new HashMap<>();
  Set<String> seenAttributes = new HashSet<>();

  public RateLimitingSpanFilter(Config config) {
    for (Config rateLimitConfig : config.getConfigList(RATE_LIMIT_CONFIG_PATH)) {
      String tenantId = rateLimitConfig.getString(TENANT_ID_KEY);
      String groupingKey = rateLimitConfig.getString(GROUPING_KEY_KEY);
      tenantMaxSpansPerMinuteMap.put(
          tenantId + groupingKey, rateLimitConfig.getLong(MAX_SPANS_PER_MINUTE_KEY));
      tenantSpanCountMap.put(tenantId + groupingKey, Map.of(0L, 0L));
      seenAttributes.add(groupingKey);
    }
  }

  public boolean shouldDropSpan(String tenantId, Event event) {
    if (seenAttributes.stream()
        .noneMatch(attribute -> tenantMaxSpansPerMinuteMap.containsKey(tenantId + attribute))) {
      return false;
    }

    for (String attribute : seenAttributes) {
      if (!event.getAttributes().getAttributeMap().containsKey(attribute)) {
        continue;
      }
      if (!tenantSpanCountMap.containsKey(tenantId + attribute)) {
        continue;
      }
      Long startTimeKey = tenantSpanCountMap.get(tenantId + attribute).keySet().iterator().next();
      if (event.getStartTimeMillis() - startTimeKey > SPAN_COUNT_WINDOW) {
        tenantSpanCountMap.put(tenantId + attribute, Map.of(event.getStartTimeMillis(), 1L));
      } else {
        long currentProcessedSpansCount =
            tenantSpanCountMap.get(tenantId + attribute).get(startTimeKey);
        if (currentProcessedSpansCount >= tenantMaxSpansPerMinuteMap.get(tenantId + attribute)) {
          return true;
        }
        tenantSpanCountMap.put(
            tenantId + attribute, Map.of(startTimeKey, currentProcessedSpansCount + 1));
      }
    }
    return false;
  }
}
