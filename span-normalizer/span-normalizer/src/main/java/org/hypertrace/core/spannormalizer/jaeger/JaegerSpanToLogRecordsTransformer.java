package org.hypertrace.core.spannormalizer.jaeger;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.Timestamps;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.Counter;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.LogEvent;
import org.hypertrace.core.datamodel.LogEvents;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.util.AttributeValueCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanToLogRecordsTransformer
    implements Transformer<byte[], PreProcessedSpan, KeyValue<String, LogEvents>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(JaegerSpanToLogRecordsTransformer.class);

  private static final String VALID_SPAN_WITH_LOGS_RECEIVED_COUNT =
      "hypertrace.reported.span.with.logs.processed";

  private static final ConcurrentMap<String, Counter> tenantToSpanWithLogsReceivedCount =
      new ConcurrentHashMap<>();

  @Override
  public void init(ProcessorContext context) {
    // no-op
  }

  @Override
  public KeyValue<String, LogEvents> transform(byte[] key, PreProcessedSpan preProcessedSpan) {
    try {
      Span value = preProcessedSpan.getSpan();
      String tenantId = preProcessedSpan.getTenantId();
      if (value.getLogsCount() == 0) {
        return null;
      }

      tenantToSpanWithLogsReceivedCount
          .computeIfAbsent(
              tenantId,
              tenant ->
                  PlatformMetricsRegistry.registerCounter(
                      VALID_SPAN_WITH_LOGS_RECEIVED_COUNT, Map.of("tenantId", tenantId)))
          .increment();

      return new KeyValue<>(null, buildLogEventRecords(value, tenantId));
    } catch (Exception e) {
      LOG.error("Exception processing log records", e);
      return null;
    }
  }

  @VisibleForTesting
  LogEvents buildLogEventRecords(Span value, String tenantId) {
    ByteBuffer spanId = value.getSpanId().asReadOnlyByteBuffer();
    ByteBuffer traceId = value.getTraceId().asReadOnlyByteBuffer();
    return LogEvents.newBuilder()
        .setLogEvents(
            value.getLogsList().stream()
                .map(
                    log ->
                        LogEvent.newBuilder()
                            .setTenantId(tenantId)
                            .setSpanId(spanId)
                            .setTraceId(traceId)
                            .setTimestampNanos(Timestamps.toNanos(log.getTimestamp()))
                            .setAttributes(buildAttributes(log.getFieldsList()))
                            .build())
                .collect(Collectors.toList()))
        .build();
  }

  private Attributes buildAttributes(List<JaegerSpanInternalModel.KeyValue> keyValues) {
    return Attributes.newBuilder()
        .setAttributeMap(
            keyValues.stream()
                .collect(
                    Collectors.toMap(
                        k -> k.getKey().toLowerCase(),
                        AttributeValueCreator::createFromJaegerKeyValue)))
        .build();
  }

  @Override
  public void close() {
    // no-op
  }
}
