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
import java.util.Collection;
import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.viewgenerator.api.RawServiceView;

public class MetricsExtractor
    implements Transformer<String, RawServiceView, KeyValue<byte[], ResourceMetrics>> {

  private LongUpDownCounter longUpDownCounter;
  private long resetCounter = 0;
  private SdkMeterProvider sdkMeterProvider;

  @Override
  public void init(ProcessorContext context) {
    SdkMeterProviderBuilder sdkMeterProviderBuilder = SdkMeterProvider.builder();
    sdkMeterProvider = sdkMeterProviderBuilder.buildAndRegisterGlobal();
    resetCounter = 0;

    Meter meter = GlobalMeterProvider.get().get("io.opentelemetry.example.metrics", "1.4.1");

    this.longUpDownCounter =
        meter
            .longUpDownCounterBuilder("num_calls")
            .setDescription("Measure the number of calls")
            .setUnit("1")
            .build();
  }

  @Override
  public KeyValue<byte[], ResourceMetrics> transform(String key, RawServiceView value) {
    Labels labels =
        Labels.of(
            "tenant_id", value.getTenantId(),
            "consumer_id", "1",
            "service_id", value.getServiceId(),
            "service_name", value.getServiceName(),
            "api_id", value.getApiId(),
            "api_name", value.getApiName());
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
