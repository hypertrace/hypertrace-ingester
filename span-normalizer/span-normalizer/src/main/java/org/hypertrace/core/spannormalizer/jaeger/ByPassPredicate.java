package org.hypertrace.core.spannormalizer.jaeger;

import com.typesafe.config.Config;
import java.util.function.Predicate;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.RawSpan;

public class ByPassPredicate implements Predicate<org.hypertrace.core.datamodel.RawSpan> {
  private static final String SPAN_BYPASSED_CONFIG = "processor.byPassFilter.key";
  private String key = null;

  public ByPassPredicate(Config jobConfig) {
    if (jobConfig.hasPath(SPAN_BYPASSED_CONFIG)) {
      key = jobConfig.getString(SPAN_BYPASSED_CONFIG);
    }
  }

  @Override
  public boolean test(RawSpan rawSpan) {
    if (key == null) return false;
    AttributeValue attributeValue = rawSpan.getEvent().getAttributes().getAttributeMap()
        .getOrDefault(key, null);
    if (attributeValue != null && attributeValue.getValue().equals("true")) {
      return true;
    }
    return false;
  }
}
