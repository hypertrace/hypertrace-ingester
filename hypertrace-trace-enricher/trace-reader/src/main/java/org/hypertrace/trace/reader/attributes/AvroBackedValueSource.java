package org.hypertrace.trace.reader.attributes;

import java.util.Optional;
import javax.annotation.Nullable;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.StructuredTrace;

abstract class AvroBackedValueSource implements ValueSource<StructuredTrace, Event> {

  protected Optional<String> getAttributeString(@Nullable Attributes attributes, String key) {
    return Optional.ofNullable(attributes)
        .map(Attributes::getAttributeMap)
        .map(attributeMap -> attributeMap.get(key))
        .map(AttributeValue::getValue);
  }

  protected Optional<Double> getMetricDouble(@Nullable Metrics metrics, String key) {
    return Optional.ofNullable(metrics)
        .map(Metrics::getMetricMap)
        .map(metricMap -> metricMap.get(key))
        .map(MetricValue::getValue);
  }
}
