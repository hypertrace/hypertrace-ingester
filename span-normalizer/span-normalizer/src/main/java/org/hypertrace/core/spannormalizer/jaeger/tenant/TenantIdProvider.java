package org.hypertrace.core.spannormalizer.jaeger.tenant;

import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;

public interface TenantIdProvider {

  Optional<String> getTenantIdTagKey();

  Optional<String> getTenantId(Map<String, KeyValue> spanTags, Map<String, KeyValue> processTags);

  void logWarning(Logger logger, Span span);
}
