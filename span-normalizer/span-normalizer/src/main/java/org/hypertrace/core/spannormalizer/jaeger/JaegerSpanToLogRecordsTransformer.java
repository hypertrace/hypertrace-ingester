package org.hypertrace.core.spannormalizer.jaeger;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.Timestamps;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.LogEvent;
import org.hypertrace.core.datamodel.LogEvents;
import org.hypertrace.core.spannormalizer.util.AttributeValueCreator;

public class JaegerSpanToLogRecordsTransformer
    implements Transformer<byte[], PreProcessedSpan, KeyValue<String, LogEvents>> {

  @Override
  public void init(ProcessorContext context) {
    // no-op
  }

  @Override
  public KeyValue<String, LogEvents> transform(byte[] key, PreProcessedSpan preProcessedSpan) {
    Span value = preProcessedSpan.getSpan();
    String tenantId = preProcessedSpan.getTenantId();
    if (value.getLogsCount() == 0) {
      return null;
    }

    return new KeyValue<>(null, buildLogEventRecords(value, tenantId));
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
