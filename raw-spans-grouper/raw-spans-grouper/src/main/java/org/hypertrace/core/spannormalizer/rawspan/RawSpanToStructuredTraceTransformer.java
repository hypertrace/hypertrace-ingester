package org.hypertrace.core.spannormalizer.rawspan;

import io.micrometer.core.instrument.Counter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.spannormalizer.TraceIdentity;

public class RawSpanToStructuredTraceTransformer
    implements Transformer<TraceIdentity, RawSpan, KeyValue<String, StructuredTrace>> {

  private static final String BYPASS_SPANS = "hypertrace.bypass.spans";
  private static final ConcurrentMap<String, Counter> tenantToBypassSpanCounter =
      new ConcurrentHashMap<>();

  @Override
  public void init(ProcessorContext context) {}

  @Override
  public KeyValue<String, StructuredTrace> transform(TraceIdentity key, RawSpan rawSpan) {

    StructuredTrace structuredTrace =
        StructuredTraceBuilder.buildStructuredTraceFromRawSpans(
            List.of(rawSpan), key.getTraceId(), key.getTenantId());

    tenantToBypassSpanCounter
        .computeIfAbsent(
            key.getTenantId(),
            tenant ->
                PlatformMetricsRegistry.registerCounter(
                    BYPASS_SPANS, Map.of("tenantId", key.getTenantId())))
        .increment();

    return new KeyValue<>(null, structuredTrace);
  }

  @Override
  public void close() {}
}
