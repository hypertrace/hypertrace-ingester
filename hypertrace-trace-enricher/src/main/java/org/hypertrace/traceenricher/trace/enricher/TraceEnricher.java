package org.hypertrace.traceenricher.trace.enricher;

import com.typesafe.config.Config;
import org.hypertrace.core.serviceframework.background.PlatformBackgroundJob;
import org.hypertrace.core.serviceframework.background.PlatformBackgroundService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceEnricher extends PlatformBackgroundService {
  private static Logger LOGGER = LoggerFactory.getLogger(TraceEnricher.class);

  public TraceEnricher(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected PlatformBackgroundJob createBackgroundJob(Config config) {
    return new StructuredTraceEnrichmentJob(config);
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
