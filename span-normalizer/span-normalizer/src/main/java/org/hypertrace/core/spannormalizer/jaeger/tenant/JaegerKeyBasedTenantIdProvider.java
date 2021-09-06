package org.hypertrace.core.spannormalizer.jaeger.tenant;

import com.google.common.util.concurrent.RateLimiter;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
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

  /**
   * gets the tenant id from process tags, with a fallback to getting it from the span tags.
   *
   * @param span
   * @param tags
   * @return the tenant id if it is present in the process tags or span tags; otherwise, returns an
   *     empty optional.
   */
  @Override
  public Optional<String> getTenantId(Span span, Map<String, KeyValue> tags) {
    return span.getProcess().getTagsList().stream()
        .filter(t -> t.getKey().equals(tenantIdKey))
        .findFirst()
        .or(() -> Optional.ofNullable(tags.get(tenantIdKey)))
        .map(KeyValue::getVStr);
  }

  @Override
  public void logWarning(Logger logger, Span span) {
    if (LOG_LIMITER.tryAcquire()) {
      logger.warn(
          "Dropping span without tenant id. tenantIdTagKey: {}, span: {}", tenantIdKey, span);
    }
  }
}
