package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

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

public class ElasticsearchBackendProviderTest {
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
  public void testBackendResolution() {
    BackendInfo backendInfo =
        backendEntityEnricher
            .resolve(getElasticsearchEvent(), structuredTrace, structuredTraceGraph)
            .get();
    Entity entity = backendInfo.getEntity();
    Assertions.assertEquals("test-index", entity.getEntityName());
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(
        Map.of(
            "BACKEND_OPERATION",
            AttributeValueCreator.create("GET"),
            "BACKEND_DESTINATION",
            AttributeValueCreator.create("test-index")),
        attributes);
  }

  @Test
  public void testBackendResolutionForOTEvent() {
    BackendInfo backendInfo =
        backendEntityEnricher
            .resolve(getElasticsearchOTEvent(), structuredTrace, structuredTraceGraph)
            .get();
    Entity entity = backendInfo.getEntity();
    Assertions.assertEquals("test", entity.getEntityName());
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(
        Map.of(
            "BACKEND_OPERATION",
            AttributeValueCreator.create("GET"),
            "BACKEND_DESTINATION",
            AttributeValueCreator.create("test-index")),
        attributes);
  }

  @Test
  public void testBackendResolutionForOTelEvent() {
    BackendInfo backendInfo =
        backendEntityEnricher
            .resolve(getElasticsearchOTelEvent(), structuredTrace, structuredTraceGraph)
            .get();
    Entity entity = backendInfo.getEntity();
    Assertions.assertEquals("test:2000", entity.getEntityName());
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(
        Map.of(
            "BACKEND_OPERATION",
            AttributeValueCreator.create("GetAction"),
            "BACKEND_DESTINATION",
            AttributeValueCreator.create("test-index")),
        attributes);
  }

  private Event getElasticsearchEvent() {
    Event event =
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
                            "elasticsearch.url",
                            AttributeValue.newBuilder().setValue("test-index").build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "db.system",
                            AttributeValue.newBuilder().setValue("elasticsearch").build(),
                            "db.operation",
                            AttributeValue.newBuilder().setValue("GET").build(),
                            "elasticsearch.method",
                            AttributeValue.newBuilder().setValue("PUT").build(),
                            "elasticsearch.action",
                            AttributeValue.newBuilder().setValue("GetAction").build(),
                            "elasticsearch.request.indices",
                            AttributeValue.newBuilder().setValue("test-index").build(),
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
            .setEventName("elasticsearch.connection")
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

  private Event getElasticsearchOTEvent() {
    Event event =
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
                            "peer.service",
                            AttributeValue.newBuilder().setValue("elasticsearch").build(),
                            "peer.hostname",
                            AttributeValue.newBuilder().setValue("test").build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "db.system",
                            AttributeValue.newBuilder().setValue("elasticsearch").build(),
                            "db.operation",
                            AttributeValue.newBuilder().setValue("GET").build(),
                            "elasticsearch.method",
                            AttributeValue.newBuilder().setValue("PUT").build(),
                            "elasticsearch.request.indices",
                            AttributeValue.newBuilder().setValue("test-index").build(),
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
            .setEventName("elasticsearch.connection")
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

  private Event getElasticsearchOTelEvent() {
    Event event =
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
                            "net.peer.port",
                            AttributeValue.newBuilder().setValue("2000").build(),
                            "net.peer.name",
                            AttributeValue.newBuilder().setValue("test").build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "db.system",
                            AttributeValue.newBuilder().setValue("elasticsearch").build(),
                            "elasticsearch.action",
                            AttributeValue.newBuilder().setValue("GetAction").build(),
                            "elasticsearch.request.indices",
                            AttributeValue.newBuilder().setValue("test-index").build(),
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
            .setEventName("elasticsearch.connection")
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
      return List.of(new ElasticSearchBackendProvider());
    }

    @Override
    public FqnResolver getFqnResolver() {
      return new HypertraceFqnResolver();
    }
  }
}
