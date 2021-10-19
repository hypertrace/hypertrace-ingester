package org.hypertrace.metrics.exporter;

import com.typesafe.config.Config;
import io.opentelemetry.exporter.prometheus.PrometheusCollector;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.metrics.exporter.consumer.MetricsKafkaConsumer;
import org.hypertrace.metrics.exporter.producer.InMemoryMetricsProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsExporterService extends PlatformService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsExporterService.class);

  private static final String BUFFER_CONFIG_KEY = "buffer.config";
  private static final String MAX_QUEUE_SIZE = "max.queue.size";

  private MetricsKafkaConsumer metricsKafkaConsumer;
  private Config config;
  private InMemoryMetricsProducer inMemoryMetricsProducer;

  public MetricsExporterService(ConfigClient configClient, Config config) {
    super(configClient);
    this.config = config;
  }

  @Override
  public void doInit() {
    config = (config != null) ? config : getAppConfig();
    int maxQueueSize = config.getConfig(BUFFER_CONFIG_KEY).getInt(MAX_QUEUE_SIZE);
    inMemoryMetricsProducer = new InMemoryMetricsProducer(maxQueueSize);
    metricsKafkaConsumer = new MetricsKafkaConsumer(config, inMemoryMetricsProducer);
    PrometheusCollector.create().apply(inMemoryMetricsProducer);
  }

  @Override
  public void doStart() {
    Thread metricsConsumerThread = new Thread(metricsKafkaConsumer);
    metricsConsumerThread.start();
    try {
      metricsConsumerThread.join();
    } catch (InterruptedException e) {
      LOGGER.error("Exception in starting the thread:", e);
    }
  }

  @Override
  public void doStop() {
    metricsKafkaConsumer.close();
  }

  @Override
  public boolean healthCheck() {
    return true;
  }
}
