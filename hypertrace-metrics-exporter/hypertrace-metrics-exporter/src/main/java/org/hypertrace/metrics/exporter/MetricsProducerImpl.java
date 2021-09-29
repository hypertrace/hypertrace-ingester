package org.hypertrace.metrics.exporter;

import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricProducer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MetricsProducerImpl implements MetricProducer {
  private ConcurrentLinkedQueue<MetricData> metricDataQueue;

  public MetricsProducerImpl() {
    this.metricDataQueue = new ConcurrentLinkedQueue();
  }

  public void addMetricData(List<MetricData> metricData) {
    this.metricDataQueue.addAll(metricData);
  }

  public Collection<MetricData> collectAllMetrics() {
    List<MetricData> metricDataList = new ArrayList<>();
    int maxItems = 0;
    while (this.metricDataQueue.peek() != null & maxItems < 1000) {
      metricDataList.add(this.metricDataQueue.poll());
    }
    return metricDataList;
  }
}
