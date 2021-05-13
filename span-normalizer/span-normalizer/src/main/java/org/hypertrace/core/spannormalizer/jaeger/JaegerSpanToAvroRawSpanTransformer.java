package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.SPAN_NORMALIZER_JOB_CONFIG;
import static org.hypertrace.core.spannormalizer.jaeger.JaegerSpanPreProcessor.SPANS_COUNTER;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.Counter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.RawSpanWrapper;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanToAvroRawSpanTransformer
    implements Transformer<byte[], PreProcessedSpan, KeyValue<TraceIdentity, RawSpanWrapper>> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(JaegerSpanToAvroRawSpanTransformer.class);

  private static final ConcurrentMap<String, Counter> statusToSpansCounter =
      new ConcurrentHashMap<>();

  private static final String VALID_SPAN_RECEIVED_COUNT = "hypertrace.reported.spans.processed";
  private static final ConcurrentMap<String, Counter> tenantToSpanReceivedCount =
      new ConcurrentHashMap<>();

  private AvroSerde<RawSpan> rawSpanAvroSerde;
  private JaegerSpanNormalizer converter;

  @Override
  public void init(ProcessorContext context) {
    rawSpanAvroSerde = (AvroSerde<RawSpan>) context.valueSerde();
    Config jobConfig = (Config) context.appConfigs().get(SPAN_NORMALIZER_JOB_CONFIG);
    converter = JaegerSpanNormalizer.get(jobConfig);
  }

  @Override
  public KeyValue<TraceIdentity, RawSpanWrapper> transform(byte[] key, PreProcessedSpan preProcessedSpan) {
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
        // we use the (tenant_id, trace_id) as the key so that raw_span_grouper
        // job can do a groupByKey without having to create a repartition topic
        TraceIdentity traceIdentity =
            TraceIdentity.newBuilder()
                .setTenantId(tenantId)
                .setTraceId(rawSpan.getTraceId())
                .build();

        return new KeyValue<>(
            traceIdentity,
            RawSpanWrapper.newBuilder()
                .setSpanId(rawSpan.getEvent().getEventId())
                .setRawSpan(ByteBuffer.wrap(rawSpanAvroSerde.serializer().serialize("", rawSpan)))
                .build());
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
