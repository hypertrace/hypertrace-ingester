package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.SPAN_NORMALIZER_JOB_CONFIG;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.Counter;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

public class JaegerSpanPreProcessor
    implements Transformer<byte[], Span, KeyValue<byte[], PreProcessedSpan>> {

  private static final String SPANS_COUNTER = "hypertrace.reported.spans";
  private static final ConcurrentMap<String, Counter> statusToSpansCounter =
      new ConcurrentHashMap<>();

  private TenantIdHandler tenantIdHandler;
  private SpanFilter spanFilter;

  public JaegerSpanPreProcessor() {
    // empty constructor
  }

  // constructor for testing
  JaegerSpanPreProcessor(Config jobConfig) {
    tenantIdHandler = new TenantIdHandler(jobConfig);
    spanFilter = new SpanFilter(jobConfig);
  }

  @Override
  public void init(ProcessorContext context) {
    Config jobConfig = (Config) context.appConfigs().get(SPAN_NORMALIZER_JOB_CONFIG);
    tenantIdHandler = new TenantIdHandler(jobConfig);
    spanFilter = new SpanFilter(jobConfig);
  }

  @Override
  public KeyValue<byte[], PreProcessedSpan> transform(byte[] key, Span value) {
    // this is total spans count received. Irrespective of the fact we are able to parse them, or
    // they have tenantId or not.
    statusToSpansCounter
        .computeIfAbsent(
            "received",
            k -> PlatformMetricsRegistry.registerCounter(SPANS_COUNTER, Map.of("result", k)))
        .increment();

    PreProcessedSpan preProcessedSpan = preProcessSpan(value);

    if (null == preProcessedSpan) {
      return null;
    }

    return new KeyValue<>(key, preProcessedSpan);
  }

  @VisibleForTesting
  PreProcessedSpan preProcessSpan(Span value) {
    Map<String, JaegerSpanInternalModel.KeyValue> tags =
        value.getTagsList().stream()
            .collect(Collectors.toMap(t -> t.getKey().toLowerCase(), t -> t, (v1, v2) -> v2));

    Optional<String> maybeTenantId = tenantIdHandler.getAllowedTenantId(value, tags);
    if (maybeTenantId.isEmpty()) {
      return null;
    }

    String tenantId = maybeTenantId.get();

    if (spanFilter.shouldDropSpan(tags)) {
      return null;
    }

    return new PreProcessedSpan(tenantId, value);
  }

  @Override
  public void close() {
    // noop
  }
}
