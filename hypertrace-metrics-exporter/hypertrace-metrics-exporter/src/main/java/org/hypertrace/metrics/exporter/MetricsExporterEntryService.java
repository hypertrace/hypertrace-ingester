package org.hypertrace.metrics.exporter;

import com.typesafe.config.Config;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsExporterEntryService extends PlatformService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsExporterEntryService.class);
  private static final String OTLP_CONFIG_KEY = "otlp.collector.config";
  private static final String PULL_EXPORTER_COFING_KEY = "pull.exporter";

  private MetricsConsumer metricsConsumer;
  private OtlpGrpcExporter otlpGrpcExporter;
  private Config config;
  private InMemoryMetricsProducer inMemoryMetricsProducer;
  private Boolean isPullExporter;
  private MetricsServer metricsServer;

  public MetricsExporterEntryService(ConfigClient configClient, Config config) {
    super(configClient);
    this.config = config;
  }

  @Override
  public void doInit() {
    config = (config != null) ? config : getAppConfig();
    inMemoryMetricsProducer = new InMemoryMetricsProducer(5000);
    metricsConsumer = new MetricsConsumer(config, inMemoryMetricsProducer);
    isPullExporter = config.getBoolean(PULL_EXPORTER_COFING_KEY);
    if (!isPullExporter) {
      otlpGrpcExporter = new OtlpGrpcExporter(config.getConfig(OTLP_CONFIG_KEY));
    } else {
      metricsServer = new MetricsServer(config, inMemoryMetricsProducer);
    }
  }

  @Override
  public void doStart() {

    Thread metricsConsumerThread = new Thread(metricsConsumer);
    Thread metricsExporterThread = null;
    if (isPullExporter) {
      metricsExporterThread = new Thread(() -> metricsServer.start());
    } else {
      metricsConsumerThread = new Thread(otlpGrpcExporter);
    }

    metricsExporterThread.start();
    metricsConsumerThread.start();

    try {
      metricsExporterThread.join();
    } catch (InterruptedException exception) {
      exception.printStackTrace();
    }

    // stop consuming if metric server thread has stopped
    metricsConsumer.stop();
    try {
      metricsConsumerThread.join();
    } catch (InterruptedException exception) {
      exception.printStackTrace();
    }
  }

  @Override
  public void doStop() {
    metricsConsumer.close();
    if (!isPullExporter) {
      otlpGrpcExporter.close();
    } else {
      metricsServer.stop();
    }
  }

  @Override
  public boolean healthCheck() {
    return true;
  }
}
