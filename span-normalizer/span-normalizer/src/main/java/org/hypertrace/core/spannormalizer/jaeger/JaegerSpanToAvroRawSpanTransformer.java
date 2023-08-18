package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.SPAN_NORMALIZER_JOB_CONFIG;
import static org.hypertrace.core.spannormalizer.jaeger.JaegerSpanPreProcessor.SPANS_COUNTER;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.Counter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.kafkastreams.framework.async.AsyncProcessor;
import org.hypertrace.core.kafkastreams.framework.async.AsyncProcessorConfig;
import org.hypertrace.core.kafkastreams.framework.async.RecordToForward;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.semantic.convention.utils.span.SpanStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanToAvroRawSpanTransformer
    extends AsyncProcessor<byte[], PreProcessedSpan, TraceIdentity, RawSpan> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(JaegerSpanToAvroRawSpanTransformer.class);

  private static final ConcurrentMap<String, Counter> statusToSpansCounter =
      new ConcurrentHashMap<>();

  private static final String VALID_SPAN_RECEIVED_COUNT = "hypertrace.reported.spans.processed";
  private static final ConcurrentMap<String, Counter> tenantToSpanReceivedCount =
      new ConcurrentHashMap<>();

  private JaegerSpanNormalizer converter;
  private static AtomicReference<SpanStore> spanStore = new AtomicReference<>();

  public JaegerSpanToAvroRawSpanTransformer(
      Supplier<Executor> executorSupplier, AsyncProcessorConfig asyncProcessorConfig) {
    super(executorSupplier, asyncProcessorConfig);
  }

  @Override
  protected void doInit(Map<String, Object> appConfigs) {
    Config jobConfig = (Config) appConfigs.get(SPAN_NORMALIZER_JOB_CONFIG);
    converter = JaegerSpanNormalizer.get(jobConfig);
    if (spanStore.get() == null) {
      synchronized (JaegerSpanToAvroRawSpanTransformer.class) {
        if (spanStore.get() == null) {
          spanStore.set(new SpanStore(jobConfig));
        }
      }
    }
  }

  @Override
  public List<RecordToForward<TraceIdentity, RawSpan>> asyncProcess(
      byte[] key, PreProcessedSpan preProcessedSpan) {
    Span value = preProcessedSpan.getSpan();
    String tenantId = preProcessedSpan.getTenantId();
    try {
      Event trimmedEvent = spanStore.get().pushToSpanStoreAndTrimSpan(preProcessedSpan.getEvent());
      RawSpan rawSpan = converter.convert(tenantId, value, trimmedEvent);
      if (null != rawSpan) {
        // these are spans per tenant that we were able to parse / convert, and had tenantId.
        tenantToSpanReceivedCount
            .computeIfAbsent(
                tenantId,
                tenant ->
                    PlatformMetricsRegistry.registerCounter(
                        VALID_SPAN_RECEIVED_COUNT, Map.of("tenantId", tenantId)))
            .increment();
        // we use the (tenant_id, trace_id) as the key so that raw_span_grouper
        // job can do a groupByKey without having to create a repartition topic
        TraceIdentity traceIdentity =
            fastNewBuilder(TraceIdentity.Builder.class)
                .setTenantId(tenantId)
                .setTraceId(rawSpan.getTraceId())
                .build();
        return List.of(RecordToForward.from(null, traceIdentity, rawSpan));
      }
      statusToSpansCounter
          .computeIfAbsent(
              "dropped",
              k -> PlatformMetricsRegistry.registerCounter(SPANS_COUNTER, Map.of("result", k)))
          .increment();
      return null;
    } catch (Exception e) {
      LOGGER.error("Error converting spans - ", e);
      statusToSpansCounter
          .computeIfAbsent(
              "error",
              k -> PlatformMetricsRegistry.registerCounter(SPANS_COUNTER, Map.of("result", k)))
          .increment();
      return null;
    }
  }

  @Override
  public void close() {}
}
