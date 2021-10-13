package org.hypertrace.metrics.exporter;

import com.typesafe.config.Config;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsServer.class);

  private Server server;

  public MetricsServer(Config config, InMemoryMetricsProducer producer) {
    server = new Server(8098);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    this.server.setHandler(context);
    this.server.setStopAtShutdown(true);
    this.server.setStopTimeout(2000L);
    //    context.addServlet(
    //        new ServletHolder(new MetricsExporterServlet(producer)), "/ingestion/metrics");
  }

  public void start() {
    try {
      this.server.start();
      LOGGER.info("Started metrics service on port: {}.", 8098);
      this.server.join();
    } catch (Exception var4) {
      LOGGER.error("Failed to start metrics servlet.");
    }
  }

  public void stop() {
    try {
      server.stop();
    } catch (Exception e) {
      LOGGER.error("Error stopping metrics server");
    }
  }
}
