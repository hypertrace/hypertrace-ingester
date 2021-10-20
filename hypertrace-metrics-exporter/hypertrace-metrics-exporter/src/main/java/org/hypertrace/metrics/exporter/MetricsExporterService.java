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

  private Config config;
  private MetricsKafkaConsumer metricsKafkaConsumer;
  private InMemoryMetricsProducer inMemoryMetricsProducer;

  public MetricsExporterService(ConfigClient configClient) {
    super(configClient);
  }

  public void setConfig(Config config) {
    this.config = config;
  }

  @Override
  public void doInit() {
    config = (config != null) ? config : getAppConfig();
    inMemoryMetricsProducer = new InMemoryMetricsProducer(config);
    metricsKafkaConsumer = new MetricsKafkaConsumer(config, inMemoryMetricsProducer);
    // TODO: Upgrade opentelemetry-exporter-prometheus to 1.8.0 release when available
    // to include time stamp related changes
    // https://github.com/open-telemetry/opentelemetry-java/pull/3700
    // For now, the exported time stamp will be the current time stamp.
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
