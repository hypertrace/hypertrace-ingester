package org.hypertrace.metrics.exporter;

import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricProducer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryMetricsProducer implements MetricProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryMetricsProducer.class);
  private BlockingQueue<MetricData> metricDataQueue;
  private final AtomicBoolean commitOffset = new AtomicBoolean(false);

  public InMemoryMetricsProducer(int maxQueueSize) {
    this.metricDataQueue = new ArrayBlockingQueue<MetricData>(maxQueueSize);
  }

  public void addMetricData(List<ResourceMetrics> resourceMetrics) {
    try {
      for (ResourceMetrics rm : resourceMetrics) {
        List<MetricData> metricData = OtlpToObjectConverter.toMetricData(rm);
        for (MetricData md : metricData) {
          this.metricDataQueue.put(md);
        }
      }
    } catch (InterruptedException exception) {
      LOGGER.info("This thread was intruppted, so we might loose copying some metrics ");
    }
  }

  public Collection<MetricData> collectAllMetrics() {
    List<MetricData> metricDataList = new ArrayList<>();
    int max = 0;
    while (max < 100 && this.metricDataQueue.peek() != null) {
      metricDataList.add(this.metricDataQueue.poll());
      max++;
    }
    return metricDataList;
  }

  public void setCommitOffset() {
    commitOffset.set(true);
  }

  public void clearCommitOffset() {
    commitOffset.set(false);
  }

  public boolean isCommitOffset() {
    return commitOffset.get();
  }
}
