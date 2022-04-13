package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.SPAN_NORMALIZER_JOB_CONFIG;
import static org.hypertrace.core.spannormalizer.util.EventBuilder.buildEvent;

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
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanPreProcessor
    implements Transformer<byte[], Span, KeyValue<byte[], PreProcessedSpan>> {
  private static final Logger LOG = LoggerFactory.getLogger(JaegerSpanPreProcessor.class);

  static final String SPANS_COUNTER = "hypertrace.reported.spans";
  private static final ConcurrentMap<String, Counter> statusToSpansCounter =
      new ConcurrentHashMap<>();
  private TenantIdHandler tenantIdHandler;
  private SpanDropManager spanDropManager;
  private TagsFilter tagsFilter;

  public JaegerSpanPreProcessor() {
    // empty constructor
  }

  @VisibleForTesting
  JaegerSpanPreProcessor(Config jobConfig, ExcludeSpanRulesCache excludeSpanRulesCache) {
    tenantIdHandler = new TenantIdHandler(jobConfig);
    spanDropManager = new SpanDropManager(jobConfig, excludeSpanRulesCache);
    tagsFilter = new TagsFilter(jobConfig);
  }

  @Override
  public void init(ProcessorContext context) {
    Config jobConfig = (Config) context.appConfigs().get(SPAN_NORMALIZER_JOB_CONFIG);
    tenantIdHandler = new TenantIdHandler(jobConfig);
    spanDropManager = new SpanDropManager(jobConfig);
    tagsFilter = new TagsFilter(jobConfig);
  }

  @Override
  public KeyValue<byte[], PreProcessedSpan> transform(byte[] key, Span value) {
    try {
      // this is total spans count received. Irrespective of the fact we are able to parse them, or
      // they have tenantId or not.
      statusToSpansCounter
          .computeIfAbsent(
              "received",
              k -> PlatformMetricsRegistry.registerCounter(SPANS_COUNTER, Map.of("result", k)))
          .increment();

      PreProcessedSpan preProcessedSpan = preProcessSpan(value);

      if (null == preProcessedSpan) {
        statusToSpansCounter
            .computeIfAbsent(
                "dropped",
                k -> PlatformMetricsRegistry.registerCounter(SPANS_COUNTER, Map.of("result", k)))
            .increment();
        return null;
      }

      return new KeyValue<>(key, preProcessedSpan);
    } catch (Exception e) {
      LOG.error("Error preprocessing span", e);
      statusToSpansCounter
          .computeIfAbsent(
              "error",
              k -> PlatformMetricsRegistry.registerCounter(SPANS_COUNTER, Map.of("result", k)))
          .increment();
      return null;
    }
  }

  @VisibleForTesting
  PreProcessedSpan preProcessSpan(Span span) {
    Map<String, JaegerSpanInternalModel.KeyValue> spanTags =
        span.getTagsList().stream()
            .collect(Collectors.toMap(t -> t.getKey().toLowerCase(), t -> t, (v1, v2) -> v2));
    Map<String, JaegerSpanInternalModel.KeyValue> processTags =
        span.getProcess().getTagsList().stream()
            .collect(Collectors.toMap(t -> t.getKey().toLowerCase(), t -> t, (v1, v2) -> v2));

    Optional<String> tenantIdMaybe =
        tenantIdHandler.getAllowedTenantId(span, spanTags, processTags);
    if (tenantIdMaybe.isEmpty()) {
      return null;
    }
    String tenantId = tenantIdMaybe.get();
    // filter tags
    Span processedSpan = tagsFilter.apply(tenantId, span);
    Event event =
        buildEvent(
            tenantId, processedSpan, tenantIdHandler.getTenantIdProvider().getTenantIdTagKey());

    if (spanDropManager.shouldDropSpan(span, event, tenantId)) {
      return null;
    }

    return new PreProcessedSpan(tenantId, processedSpan, event);
  }

  @Override
  public void close() {
    // noop
  }
}
