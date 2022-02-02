package org.hypertrace.core.spannormalizer.jaeger;

import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.StructuredTraceBuilder;
import org.hypertrace.core.spannormalizer.TraceIdentity;

public class RawSpanToStructuredTraceTransformer implements
    Transformer<TraceIdentity, RawSpan, KeyValue<String, StructuredTrace>> {

  @Override
  public void init(ProcessorContext context) {}

  @Override
  public KeyValue<String, StructuredTrace> transform(TraceIdentity key, RawSpan rawSpan) {
    StructuredTrace structuredTrace = StructuredTraceBuilder.buildStructuredTraceFromRawSpans(
        List.of(rawSpan), key.getTraceId(), key.getTenantId());

    return new KeyValue<>(null, structuredTrace);
  }

  @Override
  public void close() {

  }

}
