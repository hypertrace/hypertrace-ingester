package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.AbstractBackendEntityEnricher;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.FqnResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.HypertraceFqnResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.BackendInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SqsBackendProviderTest {
  private AbstractBackendEntityEnricher backendEntityEnricher;
  private StructuredTraceGraph structuredTraceGraph;
  private StructuredTrace structuredTrace;

  @BeforeEach
  public void setup() {
    backendEntityEnricher = new MockBackendEntityEnricher();
    backendEntityEnricher.init(ConfigFactory.empty(), mock(ClientRegistry.class));

    structuredTrace = mock(StructuredTrace.class);
    structuredTraceGraph = mock(StructuredTraceGraph.class);
  }

  @Test
  public void TestOtelSqsBackendResolution() {
    String sqsConnectionString = "https://queue.amazonaws.com/80398EXAMPLE/MyQueue";
    URL sqsURL;
    try {
      sqsURL = new URL(sqsConnectionString);
      String sqsHost = sqsURL.getHost();
      BackendInfo backendInfo =
          backendEntityEnricher
              .resolve(
                  getOtelSqsBackendEvent(sqsConnectionString),
                  structuredTrace,
                  structuredTraceGraph)
              .get();
      Entity entity = backendInfo.getEntity();
      Assertions.assertEquals(sqsHost, entity.getEntityName());
      Map<String, AttributeValue> attributes = backendInfo.getAttributes();
      assertEquals(
          Map.of(
              "BACKEND_OPERATION",
              AttributeValueCreator.create("receive"),
              "BACKEND_DESTINATION",
              AttributeValueCreator.create("QueueName")),
          attributes);
    } catch (MalformedURLException e) {
      Assertions.fail("Unable to create URL for given connection string");
    }
  }

  @Test
  public void TestOTBackendEventResolution() {
    String sqsHost = "sqs.ap-south-1.amazonaws.com";
    BackendInfo entity =
        backendEntityEnricher
            .resolve(getOTSqsBackendEvent(sqsHost), structuredTrace, structuredTraceGraph)
            .get();
    Assertions.assertEquals(sqsHost, entity.getEntity().getEntityName());
  }

  private Event getOtelSqsBackendEvent(String connectionString) {
    Event event =
        Event.newBuilder()
            .setCustomerId("customer1")
            .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f9".getBytes()))
            .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824"))
            .setEnrichedAttributes(
                Attributes.newBuilder()
                    .setAttributeMap(
                        Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build()))
                    .build())
            .setAttributes(
                Attributes.newBuilder()
                    .setAttributeMap(
                        Map.of(
                            "messaging.system",
                            AttributeValue.newBuilder().setValue("sqs").build(),
                            "messaging.url",
                            AttributeValue.newBuilder().setValue(connectionString).build(),
                            "messaging.operation",
                            AttributeValue.newBuilder().setValue("receive").build(),
                            "messaging.destination",
                            AttributeValue.newBuilder().setValue("QueueName").build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "FLAGS",
                            AttributeValue.newBuilder().setValue("0").build()))
                    .build())
            .setEventName("RecieveMessage")
            .setStartTimeMillis(1566869077746L)
            .setEndTimeMillis(1566869077750L)
            .setMetrics(
                Metrics.newBuilder()
                    .setMetricMap(
                        Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build()))
                    .build())
            .setEventRefList(
                Arrays.asList(
                    EventRef.newBuilder()
                        .setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                        .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                        .setRefType(EventRefType.CHILD_OF)
                        .build()))
            .build();

    return event;
  }

  private Event getOTSqsBackendEvent(String host) {
    Event event =
        Event.newBuilder()
            .setCustomerId("customer1")
            .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f9".getBytes()))
            .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824"))
            .setEnrichedAttributes(
                Attributes.newBuilder()
                    .setAttributeMap(
                        Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build()))
                    .build())
            .setAttributes(
                Attributes.newBuilder()
                    .setAttributeMap(
                        Map.of(
                            "peer.service", AttributeValue.newBuilder().setValue("sqs").build(),
                            "peer.hostname", AttributeValue.newBuilder().setValue(host).build(),
                            "span.kind", AttributeValue.newBuilder().setValue("client").build(),
                            "FLAGS", AttributeValue.newBuilder().setValue("0").build()))
                    .build())
            .setEventName("RecieveMessage")
            .setStartTimeMillis(1566869077746L)
            .setEndTimeMillis(1566869077750L)
            .setMetrics(
                Metrics.newBuilder()
                    .setMetricMap(
                        Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build()))
                    .build())
            .setEventRefList(
                Arrays.asList(
                    EventRef.newBuilder()
                        .setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                        .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                        .setRefType(EventRefType.CHILD_OF)
                        .build()))
            .build();

    return event;
  }

  static class MockBackendEntityEnricher extends AbstractBackendEntityEnricher {

    @Override
    public void setup(Config enricherConfig, ClientRegistry clientRegistry) {}

    @Override
    public List<BackendProvider> getBackendProviders() {
      return List.of(new SqsBackendProvider());
    }

    @Override
    public FqnResolver getFqnResolver() {
      return new HypertraceFqnResolver();
    }
  }
}
