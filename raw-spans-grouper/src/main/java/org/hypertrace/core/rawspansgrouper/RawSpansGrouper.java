package org.hypertrace.core.rawspansgrouper;

import com.typesafe.config.Config;
import org.hypertrace.core.serviceframework.background.PlatformBackgroundJob;
import org.hypertrace.core.serviceframework.background.PlatformBackgroundService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawSpansGrouper extends PlatformBackgroundService {
  private static Logger LOGGER = LoggerFactory.getLogger(RawSpansGrouper.class);

  public RawSpansGrouper(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected PlatformBackgroundJob createBackgroundJob(Config config) {
    return new RawSpanToStructuredTraceGroupingJob(config);
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
