package org.hypertrace.metrics.exporter.producer;

import com.typesafe.config.Config;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricProducer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class InMemoryMetricsProducer implements MetricProducer {

  private static final String BUFFER_CONFIG_KEY = "buffer.config";
  private static final String MAX_QUEUE_SIZE = "max.queue.size";
  private static final String MAX_BATCH_SIZE = "max.queue.size";

  private BlockingQueue<MetricData> metricDataQueue;
  private int maxQueueSize;
  private int maxBatchSize;

  public InMemoryMetricsProducer(Config config) {
    maxQueueSize = config.getConfig(BUFFER_CONFIG_KEY).getInt(MAX_QUEUE_SIZE);
    maxBatchSize = config.getConfig(BUFFER_CONFIG_KEY).getInt(MAX_BATCH_SIZE);
    this.metricDataQueue = new ArrayBlockingQueue<>(maxQueueSize);
  }

  public boolean addMetricData(List<MetricData> metricData) {
    if (this.metricDataQueue.size() + metricData.size() >= maxQueueSize) {
      return false;
    }

    for (MetricData md : metricData) {
      this.metricDataQueue.offer(md);
    }

    return true;
  }

  public Collection<MetricData> collectAllMetrics() {
    List<MetricData> metricData = new ArrayList<>();
    while (metricData.size() < maxBatchSize && this.metricDataQueue.peek() != null) {
      metricData.add(this.metricDataQueue.poll());
    }
    return metricData;
  }
}
