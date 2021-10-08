package org.hypertrace.metrics.exporter;

import io.opentelemetry.exporter.prometheus.PrometheusCollector;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class MetricsExporterServlet extends MetricsServlet {
  private InMemoryMetricsProducer inMemoryMetricsProducer;
  private MetricReader metricReader;
  private Collector collector;
  private static final CollectorRegistry collectorRegistry = new CollectorRegistry(false);

  public MetricsExporterServlet(InMemoryMetricsProducer producer) {
    super(collectorRegistry);
    collector = PrometheusCollector.getNewInstance(producer);
    collectorRegistry.register(collector);
    inMemoryMetricsProducer = producer;
  }

  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      super.doGet(req, resp);
      // List<MetricFamilySamples> test =  collector.collect();
      inMemoryMetricsProducer.setCommitOffset();
    } catch (ServletException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }
}
