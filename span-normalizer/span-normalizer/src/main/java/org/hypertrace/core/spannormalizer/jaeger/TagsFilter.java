package org.hypertrace.core.spannormalizer.jaeger;

import com.google.common.util.concurrent.RateLimiter;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
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

  public Span apply(Span span) {
    if (allowedAttributesPrefixes.isEmpty() || prefixedMatchedAllowedAttributes.isEmpty())
      return span;
    // remove extension attributes (x-) if they are not in allowedExtensionAttributes
    // and starts with http(s).request.header or http(s).response.header
    List<KeyValue> updatedTags =
        span.getTagsList().stream()
            .filter(
                keyValue -> {
                  String keyInLowerCase = keyValue.getKey().toLowerCase();
                  String matched =
                      allowedAttributesPrefixes.stream()
                          .filter(prefix -> keyInLowerCase.startsWith(prefix))
                          .findFirst()
                          .orElse(null);
                  return (matched == null
                      || prefixedMatchedAllowedAttributes.contains(keyInLowerCase));
                })
            .collect(Collectors.toList());
    return Span.newBuilder(span).clearTags().addAllTags(updatedTags).build();
  }
}
