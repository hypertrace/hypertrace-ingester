package org.hypertrace.metrics.processor;

import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsEnricher
    implements Transformer<byte[], ResourceMetrics, KeyValue<byte[], ResourceMetrics>> {
  private static final Logger logger = LoggerFactory.getLogger(MetricsEnricher.class);
  @Override
  public void init(ProcessorContext context) {}

  @Override
  public KeyValue<byte[], ResourceMetrics> transform(byte[] key, ResourceMetrics value) {
    // noop enricher for now
    logger.info("got the value in MetricsEnricher");
    return new KeyValue<>(key, value);
  }

  @Override
  public void close() {}
}
