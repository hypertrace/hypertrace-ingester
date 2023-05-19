package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
import static org.hypertrace.core.rawspansgrouper.RawSpanGrouperConstants.RAW_SPANS_GROUPER_JOB_CONFIG;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.Timestamps;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.Counter;
import java.nio.ByteBuffer;
import java.util.Collections;
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
import org.hypertrace.core.spannormalizer.util.JaegerHTTagsConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanToLogRecordsTransformer
    implements Transformer<byte[], PreProcessedSpan, KeyValue<String, LogEvents>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(JaegerSpanToLogRecordsTransformer.class);
  private static final String TENANT_IDS_TO_EXCLUDE_LOGS_CONFIG = "processor.excludeLogsTenantIds";

  private static final String VALID_SPAN_WITH_LOGS_RECEIVED_COUNT =
      "hypertrace.reported.span.with.logs.processed";

  private static final ConcurrentMap<String, Counter> tenantToSpanWithLogsReceivedCount =
      new ConcurrentHashMap<>();

  private List<String> tenantIdsToExclude;

  @Override
  public void init(ProcessorContext context) {
    Config jobConfig = (Config) context.appConfigs().get(RAW_SPANS_GROUPER_JOB_CONFIG);
    this.tenantIdsToExclude =
        jobConfig.hasPath(TENANT_IDS_TO_EXCLUDE_LOGS_CONFIG)
            ? jobConfig.getStringList(TENANT_IDS_TO_EXCLUDE_LOGS_CONFIG)
            : Collections.emptyList();
  }

  @Override
  public KeyValue<String, LogEvents> transform(byte[] key, PreProcessedSpan preProcessedSpan) {
    try {
      Span value = preProcessedSpan.getSpan();
      String tenantId = preProcessedSpan.getTenantId();
      if (value.getLogsCount() == 0 || tenantIdsToExclude.contains(tenantId)) {
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
    return fastNewBuilder(LogEvents.Builder.class)
        .setLogEvents(
            value.getLogsList().stream()
                .map(
                    log ->
                        fastNewBuilder(LogEvent.Builder.class)
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
    return fastNewBuilder(Attributes.Builder.class)
        .setAttributeMap(
            keyValues.stream()
                .collect(
                    Collectors.toMap(
                        k -> k.getKey().toLowerCase(),
                        JaegerHTTagsConverter::createFromJaegerKeyValue)))
        .build();
  }

  @Override
  public void close() {
    // no-op
  }
}
