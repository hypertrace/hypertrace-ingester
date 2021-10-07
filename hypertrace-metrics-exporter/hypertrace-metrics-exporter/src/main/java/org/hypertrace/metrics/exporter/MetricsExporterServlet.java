package org.hypertrace.metrics.exporter;

import io.opentelemetry.exporter.prometheus.PrometheusCollector;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.prometheus.client.exporter.MetricsServlet;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class MetricsExporterServlet extends MetricsServlet {
  // private PrometheusCollector prometheusCollector;
  // private static final CollectorRegistry collectorRegistry = new CollectorRegistry(false);
  private InMemoryMetricsProducer inMemoryMetricsProducer;
  private MetricReader metricReader;

  public MetricsExporterServlet(InMemoryMetricsProducer producer) {
    metricReader = PrometheusCollector.create().apply(producer);
    inMemoryMetricsProducer = producer;
  }

  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      // List<MetricFamilySamples> samples = prometheusCollector.collect();
      super.doGet(req, resp);
      inMemoryMetricsProducer.setCommitOffset();
    } catch (ServletException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }
}
