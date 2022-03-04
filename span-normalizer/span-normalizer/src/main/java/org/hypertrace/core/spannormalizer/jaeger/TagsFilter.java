package org.hypertrace.core.spannormalizer.jaeger;

import com.google.common.util.concurrent.RateLimiter;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.DistributionSummary;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagsFilter {
  private static final Logger LOG = LoggerFactory.getLogger(TagsFilter.class);
  private static final RateLimiter DROPPED_TAGS_LIMITER = RateLimiter.create(0.001);

  private static final String ALLOWED_ATTRIBUTES_PREFIXES_CONFIG_KEY =
      "processor.allowed.attributes.prefixes";
  private static final String ALLOWED_ATTRIBUTES_CONFIG_KEY =
      "processor.prefixed.matched.allowed.attributes";

  private Set<String> allowedAttributesPrefixes = new TreeSet<>();
  private Set<String> prefixedMatchedAllowedAttributes = new TreeSet<>();

  private static final String TAGS_BYTES = "hypertrace.span.tags.bytes";
  private static final String TAGS_PROCESSED_BYTES = "hypertrace.span.tags.processed.bytes";
  private static final ConcurrentMap<String, DistributionSummary> tenantToTagsTotalSize =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, DistributionSummary> tenantToTagsProcessedSize =
      new ConcurrentHashMap<>();

  public TagsFilter(Config config) {
    if (config.hasPath(ALLOWED_ATTRIBUTES_PREFIXES_CONFIG_KEY)) {
      config
          .getStringList(ALLOWED_ATTRIBUTES_PREFIXES_CONFIG_KEY)
          .forEach(e -> allowedAttributesPrefixes.add(e.toLowerCase()));
    }

    if (config.hasPath(ALLOWED_ATTRIBUTES_CONFIG_KEY)) {
      config
          .getStringList(ALLOWED_ATTRIBUTES_CONFIG_KEY)
          .forEach(e -> prefixedMatchedAllowedAttributes.add(e.toLowerCase()));
    }
  }

  /**
   * Filter out the tags based on prefix matched configuration. allowedAttributesPrefixes: List of
   * prefix keys that are consider as subsets prefixedMatchedAllowedAttributes : allowed keys from
   * the prefixed matched subset.
   *
   * <p>e.g if, we want to drop all the http request extension attributes except few one
   * allowedAttributesPrefixes = ["http.request.header.x-"] prefixedMatchedAllowedAttributes =
   * ["http.request.header.x-tenant-id"]
   *
   * <p>Based on above configuration, from all the keys that starts with "http.request.header.x-",
   * it will retains on "http.request.header.x-tenant-id". However, this will not impact any other
   * tags which don't start with the prefix. So, keys like "http.method" will all be retain.
   */
  public Span apply(String tenantId, Span span) {
    if (allowedAttributesPrefixes.isEmpty() || prefixedMatchedAllowedAttributes.isEmpty())
      return span;

    long totalTagsBytes = 0;
    long updatedTagsBytes = 0;

    List<KeyValue> updatedTags = new ArrayList<>();
    List<KeyValue> droppedTags = new ArrayList<>();

    for (KeyValue tag : span.getTagsList()) {
      long tagBytes = calculateSize(tag);
      totalTagsBytes += tagBytes;

      String keyInLowerCase = tag.getKey().toLowerCase();
      String matched =
          allowedAttributesPrefixes.stream()
              .filter(prefix -> keyInLowerCase.startsWith(prefix))
              .findFirst()
              .orElse(null);
      if (matched == null || prefixedMatchedAllowedAttributes.contains(keyInLowerCase)) {
        updatedTags.add(tag);
        updatedTagsBytes += tagBytes;
      } else {
        droppedTags.add(tag);
      }
    }

    if (LOG.isDebugEnabled() && DROPPED_TAGS_LIMITER.tryAcquire()) {
      LOG.debug("Dropped List of tags:{} for tenant:{}", droppedTags, tenantId);
    }

    recordMetrics(tenantId, totalTagsBytes, updatedTagsBytes);

    return Span.newBuilder(span).clearTags().addAllTags(updatedTags).build();
  }

  private int calculateSize(KeyValue tag) {
    switch (tag.getVType()) {
      case BINARY:
        return tag.getVBinary() != null ? tag.getVBinary().size() : 0;
      case STRING:
        return tag.getVStr() != null ? tag.getVStr().getBytes().length : 0;
      case FLOAT64:
        return Double.BYTES;
      case INT64:
        return Integer.BYTES;
      case BOOL:
        return 2;
      default:
        return 0;
    }
  }

  private void recordMetrics(String tenantId, long totalTagsBytes, long updatedTagsBytes) {
    tenantToTagsTotalSize
        .computeIfAbsent(
            tenantId,
            tenant ->
                PlatformMetricsRegistry.registerDistributionSummary(
                    TAGS_BYTES, Map.of("tenantId", tenantId)))
        .record(totalTagsBytes);

    tenantToTagsProcessedSize
        .computeIfAbsent(
            tenantId,
            tenant ->
                PlatformMetricsRegistry.registerDistributionSummary(
                    TAGS_PROCESSED_BYTES, Map.of("tenantId", tenantId)))
        .record(updatedTagsBytes);
  }
}
