package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.Timer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.RawSpan.Builder;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanNormalizer {
  private static final Logger LOG = LoggerFactory.getLogger(JaegerSpanNormalizer.class);

  /** Service name can be sent against this key as well */
  public static final String OLD_JAEGER_SERVICENAME_KEY = "jaeger.servicename";

  private static final String SPAN_NORMALIZATION_TIME_METRIC = "span.normalization.time";

  private static JaegerSpanNormalizer INSTANCE;
  private final ConcurrentMap<String, Timer> tenantToSpanNormalizationTimer =
      new ConcurrentHashMap<>();
  private final JaegerResourceNormalizer resourceNormalizer = new JaegerResourceNormalizer();
  private final TenantIdHandler tenantIdHandler;

  public static JaegerSpanNormalizer get(Config config) {
    if (INSTANCE == null) {
      synchronized (JaegerSpanNormalizer.class) {
        if (INSTANCE == null) {
          INSTANCE = new JaegerSpanNormalizer(config);
        }
      }
    }
    return INSTANCE;
  }

  public JaegerSpanNormalizer(Config config) {
    this.tenantIdHandler = new TenantIdHandler(config);
  }

  public Timer getSpanNormalizationTimer(String tenantId) {
    return tenantToSpanNormalizationTimer.get(tenantId);
  }

  @Nullable
  public RawSpan convert(String tenantId, Span jaegerSpan, Event event) throws Exception {

    // Record the time taken for converting the span, along with the tenant id tag.
    return tenantToSpanNormalizationTimer
        .computeIfAbsent(
            tenantId,
            tenant ->
                PlatformMetricsRegistry.registerTimer(
                    SPAN_NORMALIZATION_TIME_METRIC, Map.of("tenantId", tenant)))
        .recordCallable(getRawSpanNormalizerCallable(jaegerSpan, tenantId, event));
  }

  @Nonnull
  private Callable<RawSpan> getRawSpanNormalizerCallable(
      Span jaegerSpan, String tenantId, Event event) {
    return () -> {
      Builder rawSpanBuilder = fastNewBuilder(RawSpan.Builder.class);
      rawSpanBuilder.setCustomerId(tenantId);
      rawSpanBuilder.setTraceId(jaegerSpan.getTraceId().asReadOnlyByteBuffer());
      rawSpanBuilder.setEvent(event);
      rawSpanBuilder.setReceivedTimeMillis(System.currentTimeMillis());
      resourceNormalizer
          .normalize(jaegerSpan, tenantIdHandler.getTenantIdProvider().getTenantIdTagKey())
          .ifPresent(rawSpanBuilder::setResource);

      // build raw span
      RawSpan rawSpan = rawSpanBuilder.build();
      if (LOG.isDebugEnabled()) {
        logSpanConversion(jaegerSpan, rawSpan);
      }
      return rawSpan;
    };
  }

  // Check if debug log is enabled before calling this method
  private void logSpanConversion(Span jaegerSpan, RawSpan rawSpan) {
    try {
      LOG.debug(
          "Converted Jaeger span: {} to rawSpan: {} ",
          jaegerSpan,
          convertToJsonString(rawSpan, rawSpan.getSchema()));
    } catch (IOException e) {
      LOG.warn("An exception occurred while converting avro to JSON string", e);
    }
  }

  // We should have a small library for useful short methods like this one such as this.
  public static <T extends SpecificRecordBase> String convertToJsonString(T object, Schema schema)
      throws IOException {
    JsonEncoder encoder;
    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      DatumWriter<T> writer = new SpecificDatumWriter<>(schema);
      encoder = EncoderFactory.get().jsonEncoder(schema, output, false);
      writer.write(object, encoder);
      encoder.flush();
      output.flush();
      return output.toString();
    }
  }
}
