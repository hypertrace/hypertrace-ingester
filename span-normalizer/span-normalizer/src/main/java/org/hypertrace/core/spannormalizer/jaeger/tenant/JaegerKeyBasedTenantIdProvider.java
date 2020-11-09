package org.hypertrace.core.spannormalizer.jaeger.tenant;

import com.google.common.util.concurrent.RateLimiter;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;

/**
 * Implementation of {@link TenantIdProvider} which reads tenant id from the given tags based on the
 * given tenant id tag key.
 */
public class JaegerKeyBasedTenantIdProvider implements TenantIdProvider {
  private static final RateLimiter LOG_LIMITER = RateLimiter.create(0.1);

  private final String tenantIdKey;

  public JaegerKeyBasedTenantIdProvider(String tenantIdKey) {
    this.tenantIdKey = tenantIdKey;
  }

  @Override
  public Optional<String> getTenantIdTagKey() {
    return Optional.of(tenantIdKey);
  }

  @Override
  public Optional<String> getTenantId(Map<String, JaegerSpanInternalModel.KeyValue> tags) {
    JaegerSpanInternalModel.KeyValue value = tags.get(tenantIdKey);
    return Optional.ofNullable(value != null ? value.getVStr() : null);
  }

  @Override
  public void logWarning(Logger logger, JaegerSpanInternalModel.Span span) {
    if (LOG_LIMITER.tryAcquire()) {
      logger.warn(
          "Dropping span without tenant id. tenantIdTagKey: {}, span: {}", tenantIdKey, span);
    }
  }
}
