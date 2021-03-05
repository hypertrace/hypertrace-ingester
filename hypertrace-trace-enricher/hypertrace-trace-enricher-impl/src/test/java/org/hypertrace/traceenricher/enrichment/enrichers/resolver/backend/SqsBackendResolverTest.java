package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.data.service.v1.Entity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;


public class SqsBackendResolverTest {

  private SqsBackendResolver sqsBackendResolver;
  private StructuredTraceGraph structuredTraceGraph;

  @BeforeEach
  public void setup() {
    sqsBackendResolver = new SqsBackendResolver();
    structuredTraceGraph = mock(StructuredTraceGraph.class);
  }

  @Test
  public void TestSqsBackendResolution() {
    String sqsConnectionString = "https://queue.amazonaws.com/80398EXAMPLE/MyQueue";
    URL sqsURL;
    try {
      sqsURL = new URL(sqsConnectionString);
      String sqsHost = sqsURL.getHost();
      Entity entity = sqsBackendResolver.resolveEntity(getSqsBackendEvent(sqsConnectionString), structuredTraceGraph).get();
      Assertions.assertEquals(sqsHost, entity.getEntityName());
    } catch(MalformedURLException e) {
      Assertions.fail("Unable to create URL for given connection string");
    }
  }

  private Event getSqsBackendEvent(String connectionString) {
    Event event =  Event.newBuilder().setCustomerId("customer1")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f9".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("messaging.system", AttributeValue.newBuilder().setValue("sqs").build(),
                "messaging.url", AttributeValue.newBuilder().setValue(connectionString).build(),
                "span.kind", AttributeValue.newBuilder().setValue("client").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("0").build())).build())
        .setEventName("RecieveMessage").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();


    return event;
  }
}
