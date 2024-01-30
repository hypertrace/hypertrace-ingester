package org.hypertrace.core.spannormalizer.rawspan;

import com.typesafe.config.Config;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.streams.kstream.Predicate;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.spannormalizer.TraceIdentity;

public class ByPassPredicate implements Predicate<TraceIdentity, RawSpan> {
  private static final String ALL = "*";
  private static final String SPAN_BYPASSED_CONFIG = "processor.bypass.key";
  private static final String SPAN_BYPASSED_OVERRIDE_CONFIG = "processor.bypass.override.tenants";
  private final String bypassKey;
  private final List<String> bypassOverrideTenants;

  public ByPassPredicate(Config jobConfig) {
    bypassKey =
        jobConfig.hasPath(SPAN_BYPASSED_CONFIG) ? jobConfig.getString(SPAN_BYPASSED_CONFIG) : null;
    bypassOverrideTenants =
        jobConfig.hasPath(SPAN_BYPASSED_OVERRIDE_CONFIG)
            ? jobConfig.getStringList(SPAN_BYPASSED_OVERRIDE_CONFIG)
            : Collections.emptyList();
  }

  @Override
  public boolean test(TraceIdentity traceIdentity, RawSpan rawSpan) {
    // tenant level spans bypass override
    if (bypassOverrideTenants.contains(rawSpan.getCustomerId())
        || bypassOverrideTenants.contains(ALL)) {
      return false;
    }
    AttributeValue defaultAttributeValue = AttributeValue.newBuilder().setValue("false").build();
    AttributeValue attributeValue =
        bypassKey != null
            ? rawSpan
                .getEvent()
                .getAttributes()
                .getAttributeMap()
                .getOrDefault(bypassKey, defaultAttributeValue)
            : defaultAttributeValue;
    return Boolean.parseBoolean(attributeValue.getValue());
  }
}
