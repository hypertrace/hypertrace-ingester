package org.hypertrace.core.spannormalizer.jaeger.tenant;

import com.google.common.util.concurrent.RateLimiter;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;

/** Implementation of {@link TenantIdProvider} which returns the given default tenant id always. */
public class DefaultTenantIdProvider implements TenantIdProvider {
  private static final RateLimiter LOG_LIMITER = RateLimiter.create(0.1);

  private final Optional<String> defaultTenantId;

  public DefaultTenantIdProvider(String defaultTenantId) {
    this.defaultTenantId = Optional.ofNullable(defaultTenantId);
  }

  @Override
  public Optional<String> getTenantIdTagKey() {
    return Optional.empty();
  }

  @Override
  public Optional<String> getTenantId(
      Map<String, KeyValue> spanTags, Map<String, KeyValue> processTags) {
    return defaultTenantId;
  }

  @Override
  public void logWarning(Logger logger, JaegerSpanInternalModel.Span span) {
    if (LOG_LIMITER.tryAcquire()) {
      logger.warn("Dropping span without tenant id. defaultTenantId: {}", defaultTenantId);
    }
  }
}
