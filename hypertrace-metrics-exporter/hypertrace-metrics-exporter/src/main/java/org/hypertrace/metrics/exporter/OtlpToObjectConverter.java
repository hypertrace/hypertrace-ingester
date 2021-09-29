package org.hypertrace.metrics.exporter;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric.DataCase;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.data.DoubleGaugeData;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OtlpToObjectConverter {

  private Resource toResource(io.opentelemetry.proto.resource.v1.Resource otlpResource) {
    return Resource.create(toAttributes(otlpResource.getAttributesList()));
  }

  private InstrumentationLibraryInfo toInstrumentationLibraryInfo(
      InstrumentationLibrary otlpInstrumentationLibraryInfo) {
    return InstrumentationLibraryInfo.create(
        otlpInstrumentationLibraryInfo.getName(), otlpInstrumentationLibraryInfo.getVersion());
  }

  private Attributes toAttributes(List<KeyValue> keyValues) {
    AttributesBuilder attributesBuilder = Attributes.builder();
    keyValues.forEach(
        keyValue -> {
          attributesBuilder.put(keyValue.getKey(), keyValue.getValue().getStringValue());
        });
    return attributesBuilder.build();
  }

  private List<DoublePointData> toDoublePointData(List<NumberDataPoint> numberDataPoints) {
    return numberDataPoints.stream()
        .map(
            numberDataPoint ->
                DoublePointData.create(
                    numberDataPoint.getStartTimeUnixNano(),
                    numberDataPoint.getTimeUnixNano(),
                    toAttributes(numberDataPoint.getAttributesList()),
                    numberDataPoint.getAsDouble()))
        .collect(Collectors.toList());
  }

  public List<MetricData> toMetricData(ResourceMetrics resourceMetrics) {
    List<MetricData> metricData = new ArrayList<>();
    Resource resource = toResource(resourceMetrics.getResource());
    resourceMetrics
        .getInstrumentationLibraryMetricsList()
        .forEach(
            instrumentationLibraryMetrics -> {
              InstrumentationLibraryInfo instrumentationLibraryInfo =
                  toInstrumentationLibraryInfo(
                      instrumentationLibraryMetrics.getInstrumentationLibrary());
              instrumentationLibraryMetrics
                  .getMetricsList()
                  .forEach(
                      metric -> {
                        // get type : for now only support gauge
                        if (metric.getDataCase().equals(DataCase.GAUGE)) {
                          Gauge gaugeMetric = metric.getGauge();
                          String name = metric.getName();
                          String description = metric.getDescription();
                          String unit = metric.getUnit();
                          DoubleGaugeData data =
                              DoubleGaugeData.create(
                                  toDoublePointData(gaugeMetric.getDataPointsList()));
                          MetricData md =
                              MetricData.createDoubleGauge(
                                  resource,
                                  instrumentationLibraryInfo,
                                  name,
                                  description,
                                  unit,
                                  data);
                          metricData.add(md);
                        }
                      });
            });
    return List.of();
  }
}
