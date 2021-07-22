package org.hypertrace.core.spannormalizer.otel;

import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OtelMetricProcessor
    implements Transformer<byte[], ResourceMetrics, KeyValue<byte[], ResourceMetrics>> {
  private static final Logger LOG = LoggerFactory.getLogger(OtelMetricProcessor.class);

  private static Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

  @Override
  public void init(final ProcessorContext context) {}

  @Override
  public void close() {}

  @Override
  public KeyValue<byte[], ResourceMetrics> transform(
      final byte[] key, final ResourceMetrics value) {
    try {
      LOG.info(printer.print(value));
    } catch (Exception e) {
      LOG.error("parsing exception:", e);
    }
    return new KeyValue<>(key, value);
  }
}
