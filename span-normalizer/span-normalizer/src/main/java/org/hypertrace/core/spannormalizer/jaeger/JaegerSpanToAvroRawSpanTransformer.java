package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.SPAN_NORMALIZER_JOB_CONFIG;
import static org.hypertrace.core.spannormalizer.jaeger.JaegerSpanPreProcessor.SPANS_COUNTER;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanToAvroRawSpanTransformer
    implements Transformer<byte[], PreProcessedSpan, KeyValue<TraceIdentity, RawSpan>> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(JaegerSpanToAvroRawSpanTransformer.class);

  private static final ConcurrentMap<String, Counter> statusToSpansCounter =
      new ConcurrentHashMap<>();

  private static final String VALID_SPAN_RECEIVED_COUNT = "hypertrace.reported.spans.processed";
  private static final String SPAN_REQUEST_BODY_SIZE = "hypertrace.spans.request.body.size";
  private static final String SPAN_RESPONSE_BODY_SIZE = "hypertrace.spans.response.body.size";
  private static final ConcurrentMap<String, Counter> tenantToSpanReceivedCount =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, DistributionSummary> tenantToSpanRequestBodySize =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, DistributionSummary> tenantToSpanResponseBodySize =
      new ConcurrentHashMap<>();
  private JaegerSpanNormalizer converter;

  @Override
  public void init(ProcessorContext context) {
    Config jobConfig = (Config) context.appConfigs().get(SPAN_NORMALIZER_JOB_CONFIG);
    converter = JaegerSpanNormalizer.get(jobConfig);
  }

  @Override
  public KeyValue<TraceIdentity, RawSpan> transform(byte[] key, PreProcessedSpan preProcessedSpan) {
    Span value = preProcessedSpan.getSpan();
    String tenantId = preProcessedSpan.getTenantId();
    try {
      RawSpan rawSpan = converter.convert(tenantId, value);
      if (null != rawSpan) {
        // these are spans per tenant that we were able to parse / convert, and had tenantId.
        tenantToSpanReceivedCount
            .computeIfAbsent(
                tenantId,
                tenant ->
                    PlatformMetricsRegistry.registerCounter(
                        VALID_SPAN_RECEIVED_COUNT, Map.of("tenantId", tenantId)))
            .increment();

        // Record request body size per tenantId
        Optional<Integer> requestSize = getRequestSize(rawSpan.getEvent());
        if (requestSize.isPresent()) {
          tenantToSpanRequestBodySize
              .computeIfAbsent(
                  tenantId,
                  tenant ->
                      PlatformMetricsRegistry.registerDistributionSummary(
                          SPAN_REQUEST_BODY_SIZE, Map.of("tenantId", tenantId)))
              .record(requestSize.get().doubleValue());
        }

        // Record response body size per tenantId
        Optional<Integer> responseSize = getResponseSize(rawSpan.getEvent());
        if (responseSize.isPresent()) {
          tenantToSpanResponseBodySize
              .computeIfAbsent(
                  tenantId,
                  tenant ->
                      PlatformMetricsRegistry.registerDistributionSummary(
                          SPAN_RESPONSE_BODY_SIZE, Map.of("tenantId", tenantId)))
              .record(requestSize.get().doubleValue());
        }

        // we use the (tenant_id, trace_id) as the key so that raw_span_grouper
        // job can do a groupByKey without having to create a repartition topic
        TraceIdentity traceIdentity =
            TraceIdentity.newBuilder()
                .setTenantId(tenantId)
                .setTraceId(rawSpan.getTraceId())
                .build();
        return new KeyValue<>(traceIdentity, rawSpan);
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

  private Optional<Integer> getRequestSize(Event event) {
    if (event.getHttp() != null && event.getHttp().getRequest() != null) {
      return Optional.of(event.getHttp().getRequest().getSize());
    }
    if (event.getGrpc() != null && event.getGrpc().getRequest() != null) {
      return Optional.of(event.getGrpc().getRequest().getSize());
    }
    return Optional.empty();
  }

  private Optional<Integer> getResponseSize(Event event) {
    if (event.getHttp() != null && event.getHttp().getResponse() != null) {
      return Optional.of(event.getHttp().getResponse().getSize());
    }
    if (event.getGrpc() != null && event.getGrpc().getResponse() != null) {
      return Optional.of(event.getGrpc().getResponse().getSize());
    }
    return Optional.empty();
  }
}
