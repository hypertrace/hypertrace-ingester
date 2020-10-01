package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.SPAN_NORMALIZER_JOB_CONFIG;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanToAvroRawSpanTransformer implements
    Transformer<byte[], Span, KeyValue<TraceIdentity, RawSpan>> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(JaegerSpanToAvroRawSpanTransformer.class);

  private JaegerSpanNormalizer converter;

  @Override
  public void init(ProcessorContext context) {
    Config jobConfig = (Config) context.appConfigs().get(SPAN_NORMALIZER_JOB_CONFIG);
    converter = JaegerSpanNormalizer.get(jobConfig);
  }

  @Override
  public KeyValue<TraceIdentity, RawSpan> transform(byte[] key, Span value) {
    try {
      RawSpan rawSpan = converter.convert(value);
      if (null != rawSpan) {
        String traceId = HexUtils.getHex(rawSpan.getTraceId());
        String tenantId = rawSpan.getCustomerId();
        // we use the (tenant_id, trace_id) as the key so that raw_span_grouper
        // job can do a groupByKey without having to create a repartition topic
        TraceIdentity traceIdentity = TraceIdentity.newBuilder().setTenantId(tenantId)
            .setTraceId(rawSpan.getTraceId()).build();
        return new KeyValue<>(traceIdentity, rawSpan);
      }
      return null;
    } catch (Exception e) {
      LOGGER.error("Error converting spans - ", e);
      return null;
    }
  }

  @Override
  public void close() {
  }
}
