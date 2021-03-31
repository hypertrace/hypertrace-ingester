package org.hypertrace.core.spannormalizer.jaeger;

import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.LogEventRecords;
import org.hypertrace.core.datamodel.RawSpan;

public class JaegerSpanToLogRecordsTransformer implements
    Transformer<byte[], Span, KeyValue<String, LogEventRecords>> {

  @Override
  public void init(ProcessorContext context) {

  }

  @Override
  public KeyValue<String, LogEventRecords> transform(byte[] key, Span value) {
    value.g
    return null;
  }

  @Override
  public void close() {

  }
}
