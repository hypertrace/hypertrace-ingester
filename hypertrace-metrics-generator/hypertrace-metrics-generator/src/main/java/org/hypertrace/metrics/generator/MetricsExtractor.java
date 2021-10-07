package org.hypertrace.metrics.generator;

import io.opentelemetry.api.metrics.GlobalMeterProvider;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.exporter.otlp.internal.MetricAdapter;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.viewgenerator.api.RawServiceView;

public class MetricsExtractor
    implements Transformer<String, RawServiceView, KeyValue<byte[], ResourceMetrics>> {

  Map<MetricIdentity, Long> metricIdentityMap = new LinkedHashMap<>();
  long resetCounter = 0;

  @Override
  public void init(ProcessorContext context) {
    metricIdentityMap = new LinkedHashMap<>();
    resetCounter = 0;

  }

  @Override
  public KeyValue<byte[], ResourceMetrics> transform(String key, RawServiceView value) {

    Map<String, String> attributes = new HashMap<>();
    attributes.put("tenant_id", value.getTenantId());
    attributes.put("consumer_id", "1");
    attributes.put("service_id", value.getServiceId());
    attributes.put("service_name", value.getServiceName());
    attributes.put("api_id", value.getApiId());
    attributes.put("api_name", value.getApiName());

    Metric metric = new Metric("num_calls", attributes);

    Instant instant = Instant.ofEpochMilli(value.getStartTimeMillis()).truncatedTo(ChronoUnit.SECONDS);
    instant.getNano()

    longUpDownCounter.add(value.getNumCalls(), labels);
    if (resetCounter % 10 == 0) {
      resetCounter = 0;
      Collection<MetricData> metricData = sdkMeterProvider.collectAllMetrics();
      List<ResourceMetrics> resourceMetrics = MetricAdapter.toProtoResourceMetrics(metricData);
      if (resourceMetrics.size() > 0) {
        return new KeyValue<>(null, resourceMetrics.get(0));
      }
    }
    return null;
  }

  @Override
  public void close() {}
}
