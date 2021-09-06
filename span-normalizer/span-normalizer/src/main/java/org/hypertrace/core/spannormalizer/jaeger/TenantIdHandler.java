package org.hypertrace.core.spannormalizer.jaeger;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.spannormalizer.jaeger.tenant.DefaultTenantIdProvider;
import org.hypertrace.core.spannormalizer.jaeger.tenant.JaegerKeyBasedTenantIdProvider;
import org.hypertrace.core.spannormalizer.jaeger.tenant.TenantIdProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantIdHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TenantIdHandler.class);

  /** Config for providing the tag key in which the tenant id will be given in the span. */
  private static final String TENANT_ID_TAG_KEY_CONFIG = "processor.tenantIdTagKey";

  /**
   * The config to provide a default static tenant id, in case the {@link #TENANT_ID_TAG_KEY_CONFIG}
   * is not given and tenant id isn't driven by span tags. These two configs are mutually exclusive.
   */
  private static final String DEFAULT_TENANT_ID_CONFIG = "processor.defaultTenantId";

  // list of tenant ids to exclude
  private static final String TENANT_IDS_TO_EXCLUDE_CONFIG = "processor.excludeTenantIds";

  private final TenantIdProvider tenantIdProvider;
  private final List<String> tenantIdsToExclude;

  public TenantIdHandler(Config config) {
    this.tenantIdProvider = getTenantIdProvider(config);

    this.tenantIdsToExclude =
        config.hasPath(TENANT_IDS_TO_EXCLUDE_CONFIG)
            ? config.getStringList(TENANT_IDS_TO_EXCLUDE_CONFIG)
            : Collections.emptyList();

    if (!this.tenantIdsToExclude.isEmpty()) {
      LOG.info("list of tenant ids to exclude : {}", this.tenantIdsToExclude);
    }
  }

  private TenantIdProvider getTenantIdProvider(Config config) {
    // These two configs are mutually exclusive to fail if both of them exist.
    if (config.hasPath(TENANT_ID_TAG_KEY_CONFIG) && config.hasPath(DEFAULT_TENANT_ID_CONFIG)) {
      throw new RuntimeException(
          "Both "
              + TENANT_ID_TAG_KEY_CONFIG
              + " and "
              + DEFAULT_TENANT_ID_CONFIG
              + " configs shouldn't exist at same time.");
    }

    // Tag key in which the tenant id is received in the jaeger span.
    String tenantIdTagKey =
        config.hasPath(TENANT_ID_TAG_KEY_CONFIG)
            ? config.getString(TENANT_ID_TAG_KEY_CONFIG)
            : null;
    // Default static tenant id value to be used when tenant id isn't coming in the spans.
    String defaultTenantIdValue =
        config.hasPath(DEFAULT_TENANT_ID_CONFIG)
            ? config.getString(DEFAULT_TENANT_ID_CONFIG)
            : null;

    // If both the configs are null, the processor can't work so fail.
    if (tenantIdTagKey == null && defaultTenantIdValue == null) {
      throw new RuntimeException(
          "Both "
              + TENANT_ID_TAG_KEY_CONFIG
              + " and "
              + DEFAULT_TENANT_ID_CONFIG
              + " configs can't be null.");
    }

    if (tenantIdTagKey != null) {
      return new JaegerKeyBasedTenantIdProvider(tenantIdTagKey);
    } else {
      return new DefaultTenantIdProvider(defaultTenantIdValue);
    }
  }

  Optional<String> getAllowedTenantId(Span jaegerSpan, Map<String, KeyValue> tags) {
    Optional<String> maybeTenantId = this.tenantIdProvider.getTenantId(jaegerSpan, tags);

    if (maybeTenantId.isEmpty()) {
      tenantIdProvider.logWarning(LOG, jaegerSpan);
      return Optional.empty();
    }

    String tenantId = maybeTenantId.get();

    if (this.tenantIdsToExclude.contains(tenantId)) {
      LOG.debug("Dropping span for tenant id : {}", tenantId);
      return Optional.empty();
    }

    return Optional.of(tenantId);
  }

  TenantIdProvider getTenantIdProvider() {
    return tenantIdProvider;
  }
}
