package org.hypertrace.core.spannormalizer.jaeger.tenant;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;

public interface TenantIdProvider {
  Optional<String> getTenantIdTagKey();

  Optional<String> getTenantId(Map<String, JaegerSpanInternalModel.KeyValue> tags);

  void logWarning(Logger logger, JaegerSpanInternalModel.Span span);
}
