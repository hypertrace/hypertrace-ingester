package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.constants.v1.ServiceAttribute;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.AbstractBackendEntityEnricher;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.FqnResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.HypertraceFqnResolver;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClientSpanEndpointProviderTest {
  private static final String SERVICE_NAME_ATTR =
      EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_NAME);

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
  public void checkFallbackBackendEntityGeneratedFromClientExitSpan() {
    Event e =
        Event.newBuilder()
            .setCustomerId("__default")
            .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
            .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824"))
            .setEnrichedAttributes(
                fastNewBuilder(Attributes.Builder.class)
                    .setAttributeMap(
                        Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build()))
                    .build())
            .setAttributes(
                fastNewBuilder(Attributes.Builder.class)
                    .setAttributeMap(
                        Map.of(
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "k8s.pod_id",
                            AttributeValue.newBuilder()
                                .setValue("55636196-c840-11e9-a417-42010a8a0064")
                                .build(),
                            "docker.container_id",
                            AttributeValue.newBuilder()
                                .setValue(
                                    "ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f")
                                .build(),
                            "FLAGS",
                            AttributeValue.newBuilder().setValue("0").build()))
                    .build())
            .setEventName("redis::getDrivers")
            .setStartTimeMillis(1566869077746L)
            .setEndTimeMillis(1566869077750L)
            .setMetrics(
                Metrics.newBuilder()
                    .setMetricMap(
                        Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build()))
                    .build())
            .setServiceName("redis")
            .setEventRefList(
                Arrays.asList(
                    EventRef.newBuilder()
                        .setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                        .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                        .setRefType(EventRefType.CHILD_OF)
                        .build()))
            .build();

    Event parentEvent =
        Event.newBuilder()
            .setCustomerId("__default")
            .setEventId(ByteBuffer.wrap("random".getBytes()))
            .setAttributes(
                fastNewBuilder(Attributes.Builder.class)
                    .setAttributeMap(
                        Map.of("span.kind", AttributeValue.newBuilder().setValue("server").build()))
                    .build())
            .setEnrichedAttributes(
                fastNewBuilder(Attributes.Builder.class)
                    .setAttributeMap(
                        Map.of(
                            SERVICE_NAME_ATTR,
                            AttributeValue.newBuilder().setValue("customer").build()))
                    .build())
            .setEventName("getDrivers")
            .setStartTimeMillis(1566869077746L)
            .build();

    when(structuredTraceGraph.getParentEvent(e)).thenReturn(parentEvent);
    Entity backendEntity =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get().getEntity();
    assertEquals(
        "redis",
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST))
            .getValue()
            .getString());
  }

  @Test
  public void
      checkFallbackBackendEntityGeneratedFromClientExitSpanServiceNameSameAsParentServiceName() {
    Event e =
        Event.newBuilder()
            .setCustomerId("__default")
            .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
            .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824"))
            .setEnrichedAttributes(
                fastNewBuilder(Attributes.Builder.class)
                    .setAttributeMap(
                        Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build()))
                    .build())
            .setAttributes(
                fastNewBuilder(Attributes.Builder.class)
                    .setAttributeMap(
                        Map.of(
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "k8s.pod_id",
                            AttributeValue.newBuilder()
                                .setValue("55636196-c840-11e9-a417-42010a8a0064")
                                .build(),
                            "docker.container_id",
                            AttributeValue.newBuilder()
                                .setValue(
                                    "ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f")
                                .build(),
                            "FLAGS",
                            AttributeValue.newBuilder().setValue("0").build()))
                    .build())
            .setEventName("redis::getDrivers")
            .setStartTimeMillis(1566869077746L)
            .setEndTimeMillis(1566869077750L)
            .setMetrics(
                Metrics.newBuilder()
                    .setMetricMap(
                        Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build()))
                    .build())
            .setServiceName("redis")
            .setEventRefList(
                Arrays.asList(
                    EventRef.newBuilder()
                        .setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                        .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                        .setRefType(EventRefType.CHILD_OF)
                        .build()))
            .build();

    Event parentEvent =
        Event.newBuilder()
            .setCustomerId("__default")
            .setEventId(ByteBuffer.wrap("random".getBytes()))
            .setAttributes(
                fastNewBuilder(Attributes.Builder.class)
                    .setAttributeMap(
                        Map.of("span.kind", AttributeValue.newBuilder().setValue("server").build()))
                    .build())
            .setEnrichedAttributes(
                fastNewBuilder(Attributes.Builder.class)
                    .setAttributeMap(
                        Map.of(
                            SERVICE_NAME_ATTR,
                            AttributeValue.newBuilder().setValue("redis").build()))
                    .build())
            .setEventName("getDrivers")
            .setStartTimeMillis(1566869077746L)
            .build();

    // Since the event serviceName is same as parentServiceName, no new service entity should be
    // created.
    when(structuredTraceGraph.getParentEvent(e)).thenReturn(parentEvent);
    Assertions.assertTrue(
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).isEmpty());
  }

  static class MockBackendEntityEnricher extends AbstractBackendEntityEnricher {

    @Override
    public void setup(Config enricherConfig, ClientRegistry clientRegistry) {}

    @Override
    public List<BackendProvider> getBackendProviders() {
      return List.of(new ClientSpanEndpointProvider());
    }

    @Override
    public FqnResolver getFqnResolver() {
      return new HypertraceFqnResolver();
    }
  }
}
