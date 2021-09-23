package org.hypertrace.metrics.exporter;

import com.typesafe.config.Config;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.sdk.common.CompletableResultCode;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsExporter extends PlatformService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsExporter.class);
  private static final String OTLP_CONFIG_KEY = "otlp.collector.config";

  private MetricsConsumer metricsConsumer;
  private OtlpGrpcExporter otlpGrpcExporter;
  private Config config;

  public MetricsExporter(ConfigClient configClient, Config config) {
    super(configClient);
    this.config = config;
  }

  @Override
  public void doInit() {
    config = (config != null) ? config : getAppConfig();
    metricsConsumer = new MetricsConsumer(config);
    otlpGrpcExporter = new OtlpGrpcExporter(config.getConfig(OTLP_CONFIG_KEY));
  }

  @Override
  public void doStart() {
    while (true) {
      List<ResourceMetrics> resourceMetrics = metricsConsumer.consume();
      if (!resourceMetrics.isEmpty()) {
        CompletableResultCode result = otlpGrpcExporter.export(resourceMetrics);
        result.join(1, TimeUnit.MINUTES);
      }
      waitForSec(1);
    }
  }

  @Override
  public void doStop() {
    metricsConsumer.close();
    otlpGrpcExporter.close();
  }

  @Override
  public boolean healthCheck() {
    return true;
  }

  private void waitForSec(int secs) {
    try {
      Thread.sleep(1000L * secs);
    } catch (InterruptedException e) {
      LOGGER.debug("waiting for pushing next records were intruppted");
    }
  }
}
