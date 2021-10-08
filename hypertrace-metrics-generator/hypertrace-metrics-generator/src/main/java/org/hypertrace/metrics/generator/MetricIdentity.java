package org.hypertrace.metrics.generator;

import java.util.Objects;

public class MetricIdentity {
  long timeStampSecs;
  String metricKey;
  Metric metric;

  public MetricIdentity(long timeStampSecs, String metricKey, Metric metric) {
    this.timeStampSecs = timeStampSecs;
    this.metricKey = metricKey;
    this.metric = metric;
  }

  public long getTimeStampSecs() {
    return timeStampSecs;
  }

  public String getMetricKey() {
    return metricKey;
  }

  public Metric getMetric() {
    return metric;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeStampSecs, metricKey);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof MetricIdentity)) return false;
    MetricIdentity other = (MetricIdentity) o;

    return this.timeStampSecs == other.timeStampSecs && this.metricKey == other.metricKey;
  }
}
