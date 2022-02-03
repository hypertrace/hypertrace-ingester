package org.hypertrace.core.spannormalizer.rawspan;

import com.typesafe.config.Config;

import org.apache.kafka.streams.kstream.Predicate;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.spannormalizer.TraceIdentity;

public class ByPassPredicate implements Predicate<TraceIdentity, RawSpan> {
  private static final String SPAN_BYPASSED_CONFIG = "processor.bypass.key";
  private String bypassKey;

  public ByPassPredicate(Config jobConfig) {
    bypassKey = jobConfig.hasPath(SPAN_BYPASSED_CONFIG) ?
        jobConfig.getString(SPAN_BYPASSED_CONFIG) : null;
  }

  @Override
  public boolean test(TraceIdentity traceIdentity, RawSpan rawSpan) {
    AttributeValue defaultAttributeValue = AttributeValue.newBuilder().setValue("false").build();
    AttributeValue attributeValue = bypassKey != null ? rawSpan.getEvent().getAttributes()
        .getAttributeMap().getOrDefault(bypassKey, defaultAttributeValue) : defaultAttributeValue;
    return Boolean.parseBoolean(attributeValue.getValue());
  }
}
