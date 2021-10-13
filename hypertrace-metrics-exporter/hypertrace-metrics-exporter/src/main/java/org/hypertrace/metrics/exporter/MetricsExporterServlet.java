package org.hypertrace.metrics.exporter;

import io.opentelemetry.exporter.prometheus.PrometheusCollector;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;

public class MetricsExporterServlet extends MetricsServlet {
  private InMemoryMetricsProducer inMemoryMetricsProducer;
  private Collector collector;
  private static final CollectorRegistry collectorRegistry = new CollectorRegistry(false);

  public MetricsExporterServlet(InMemoryMetricsProducer producer) {
    super(collectorRegistry);
    collector = PrometheusCollector.getNewInstance(producer);
    collectorRegistry.register(collector);
    inMemoryMetricsProducer = producer;
  }
}
