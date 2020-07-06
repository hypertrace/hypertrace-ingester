package org.hypertrace.core.spannormalizer;

import com.typesafe.config.Config;
import org.hypertrace.core.serviceframework.background.PlatformBackgroundJob;
import org.hypertrace.core.serviceframework.background.PlatformBackgroundService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpanNormalizer extends PlatformBackgroundService {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpanNormalizer.class);

  public SpanNormalizer(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected PlatformBackgroundJob createBackgroundJob(Config config) {
    return new SpanNormalizerJob(config);
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
