package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.SPAN_NORMALIZER_JOB_CONFIG;

import com.google.protobuf.util.Timestamps;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.LogEventRecord;
import org.hypertrace.core.datamodel.LogEventRecords;
import org.hypertrace.core.spannormalizer.util.AttributeValueCreator;

public class JaegerSpanToLogRecordsTransformer
    implements Transformer<byte[], Span, KeyValue<String, LogEventRecords>> {

  private TenantIdHandler tenantIdHandler;
  private SpanFilter spanFilter;

  @Override
  public void init(ProcessorContext context) {
    Config jobConfig = (Config) context.appConfigs().get(SPAN_NORMALIZER_JOB_CONFIG);
    tenantIdHandler = new TenantIdHandler(jobConfig);
    spanFilter = new SpanFilter(jobConfig);
  }

  @Override
  public KeyValue<String, LogEventRecords> transform(byte[] key, Span value) {
    if (value.getLogsCount() == 0) {
      return null;
    }
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

    return new KeyValue<>(null, buildLogEventRecords(value, tenantId));
  }

  LogEventRecords buildLogEventRecords(Span value, String tenantId) {
    ByteBuffer spanId = value.getSpanId().asReadOnlyByteBuffer();
    ByteBuffer traceId = value.getTraceId().asReadOnlyByteBuffer();
    return LogEventRecords.newBuilder()
        .setLogEventRecords(
            value.getLogsList().stream()
                .map(
                    log ->
                        LogEventRecord.newBuilder()
                            .setCustomerId(tenantId)
                            .setSpanId(spanId)
                            .setTraceId(traceId)
                            .setTimeStamp(Timestamps.toMillis(log.getTimestamp()))
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
