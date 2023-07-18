package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.mockito.Mockito.mock;

import com.typesafe.config.ConfigFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.util.EnricherInternalExceptionType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class EnricherWithExceptionMetricsTest {

  @BeforeAll
  static void setup() {
    PlatformMetricsRegistry.initMetricsRegistry(
        "TestEnricherTest", ConfigFactory.parseMap(Map.of("reporter.names", List.of("testing"))));
  }

  @Test
  void testExceptionCountMetricsAreEmitted() {
    TestEnricher enricher = new TestEnricher();
    Event e = mock(Event.class);
    StructuredTrace trace =
        StructuredTrace.newBuilder()
            .setCustomerId("testcustomer")
            .setTraceId(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()))
            .setEntityList(new ArrayList<>())
            .setEntityEdgeList(new ArrayList<>())
            .setEventEdgeList(new ArrayList<>())
            .setEntityEventEdgeList(new ArrayList<>())
            .setEventList(new ArrayList<>())
            .build();
    enricher.enrichEvent(trace, e);

    double processingExceptionCount =
        PlatformMetricsRegistry.getMeterRegistry()
            .counter(
                "hypertrace.trace.enrichment.internal.exceptions",
                "enricher",
                "TestEnricher",
                "tenantId",
                "testcustomer",
                "exception",
                "processing.exception")
            .count();

    Assertions.assertEquals(1d, processingExceptionCount);
  }
}

class TestEnricher extends AbstractTraceEnricher {
  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    if (trace.getAttributes() == null) {
      trackExceptions(trace, EnricherInternalExceptionType.PROCESS_EXCEPTION);
    }
  }
}
