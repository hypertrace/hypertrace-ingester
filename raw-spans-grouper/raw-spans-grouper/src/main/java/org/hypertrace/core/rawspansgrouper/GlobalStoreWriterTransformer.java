package org.hypertrace.core.rawspansgrouper;

import java.nio.ByteBuffer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.spannormalizer.SpanIdentity;
import org.hypertrace.core.spannormalizer.TraceIdentity;

public class GlobalStoreWriterTransformer
    implements Transformer<TraceIdentity, RawSpan, KeyValue<SpanIdentity, RawSpan>> {
  @Override
  public void init(ProcessorContext context) {}

  @Override
  public KeyValue<SpanIdentity, RawSpan> transform(TraceIdentity key, RawSpan value) {
    String tenantId = key.getTenantId();
    ByteBuffer traceId = value.getTraceId();
    ByteBuffer spanId = value.getEvent().getEventId();
    SpanIdentity spanIdentity = new SpanIdentity(tenantId, traceId, spanId);
    return KeyValue.pair(spanIdentity, value);
  }

  @Override
  public void close() {}
}
