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

  Map<String, Map<Long, Long>> tenantSpanCountMap = new HashMap<>();
  Map<String, Long> tenantMaxSpansPerMinuteMap = new HashMap<>();
  Set<String> seenAttributes = new HashSet<>();

  public RateLimitingSpanFilter(Config config) {
    for (Config rateLimitConfig : config.getConfigList("rate.limit.config")) {
      String tenantId = rateLimitConfig.getString("tenantId");
      String groupingKey = rateLimitConfig.getString("groupingKey");
      tenantMaxSpansPerMinuteMap.put(
          tenantId + groupingKey, rateLimitConfig.getLong("maxSpansPerMinute"));
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
      Long startTimeKey = tenantSpanCountMap.get(tenantId + attribute).keySet().iterator().next();
      if (event.getStartTimeMillis() - startTimeKey > 3600) {
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
