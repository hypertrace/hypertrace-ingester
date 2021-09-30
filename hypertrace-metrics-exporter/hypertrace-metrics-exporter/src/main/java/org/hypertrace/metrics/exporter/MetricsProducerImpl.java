package org.hypertrace.metrics.exporter;

import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricProducer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsProducerImpl implements MetricProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsProducerImpl.class);
  private BlockingQueue<MetricData> metricDataQueue;

  public MetricsProducerImpl(int maxQueueSize) {
    this.metricDataQueue = new ArrayBlockingQueue<MetricData>(maxQueueSize);
  }

  public void addMetricData(List<MetricData> metricData) throws InterruptedException {
    this.metricDataQueue.addAll(metricData);
    for (MetricData md : metricData) {
      this.metricDataQueue.put(md);
    }
  }

  public Collection<MetricData> collectAllMetrics() {
    List<MetricData> metricDataList = new ArrayList<>();
    this.metricDataQueue.drainTo(metricDataList);
    return metricDataList;
  }
}
