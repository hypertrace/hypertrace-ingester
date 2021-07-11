package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
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
import org.hypertrace.core.span.constants.v1.Grpc;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.AbstractBackendEntityEnricher;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.FqnResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.HypertraceFqnResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.BackendInfo;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GrpcBackendProviderTest {
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
  public void checkBackendEntityGeneratedFromGrpcEvent() {
    Map<String, AttributeValue> attributeMap =
        ImmutableMap.<String, AttributeValue>builder()
            .put(
                "grpc.method",
                AttributeValue.newBuilder()
                    .setValue("/hipstershop.ProductCatalogService/ListProducts")
                    .build())
            .put("span.kind", AttributeValue.newBuilder().setValue("client").build())
            .put("component", AttributeValue.newBuilder().setValue("grpc").build())
            .put(
                "k8s.pod_id",
                AttributeValue.newBuilder()
                    .setValue("55636196-c840-11e9-a417-42010a8a0064")
                    .build())
            .put(
                "docker.container_id",
                AttributeValue.newBuilder()
                    .setValue("ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f")
                    .build())
            .put("FLAGS", AttributeValue.newBuilder().setValue("0").build())
            .put(
                "grpc.host_port",
                AttributeValue.newBuilder().setValue("productcatalogservice:3550").build())
            .put(
                "grpc.response.body",
                AttributeValue.newBuilder()
                    .setValue(
                        "products {\\n  id: \\\"5d644175551847d7408760b5\\\"\\n  name: \\\"Vintage Record Player\\\"\\n  description: \\\"It still works.\\\"\\n  picture: \\\"/static")
                    .build())
            .put("grpc.request.body", AttributeValue.newBuilder().setValue("").build())
            .build();
    Event e =
        Event.newBuilder()
            .setCustomerId("__default")
            .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
            .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824"))
            .setEnrichedAttributes(
                Attributes.newBuilder()
                    .setAttributeMap(
                        Map.of(
                            "SPAN_TYPE",
                            AttributeValue.newBuilder().setValue("EXIT").build(),
                            "rpc.service",
                            AttributeValue.newBuilder().setValue("myservice.EchoService").build(),
                            "PROTOCOL",
                            AttributeValue.newBuilder().setValue("GRPC").build()))
                    .build())
            .setAttributes(Attributes.newBuilder().setAttributeMap(attributeMap).build())
            .setEventName("Sent.hipstershop.ProductCatalogService.ListProducts")
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
    final BackendInfo backendInfo =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get();
    final Entity backendEntity = backendInfo.getEntity();
    assertEquals("productcatalogservice:3550", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue()
            .getString(),
        "GRPC");
    assertEquals(
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST))
            .getValue()
            .getString(),
        "productcatalogservice");
    assertEquals(
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT))
            .getValue()
            .getString(),
        "3550");
    assertEquals(
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT))
            .getValue()
            .getString(),
        "Sent.hipstershop.ProductCatalogService.ListProducts");
    assertEquals(
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID))
            .getValue()
            .getString(),
        "62646630336466616266356337306638");
    assertEquals(
        backendEntity
            .getAttributesMap()
            .get(Constants.getRawSpanConstant(Grpc.GRPC_METHOD))
            .getValue()
            .getString(),
        "/hipstershop.ProductCatalogService/ListProducts");
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(
        Map.of(
            "BACKEND_DESTINATION",
            AttributeValueCreator.create("myservice.EchoService"),
            "BACKEND_OPERATION",
            AttributeValueCreator.create("/hipstershop.ProductCatalogService/ListProducts")),
        attributes);
  }

  static class MockBackendEntityEnricher extends AbstractBackendEntityEnricher {

    @Override
    public void setup(Config enricherConfig, ClientRegistry clientRegistry) {}

    @Override
    public List<BackendProvider> getBackendProviders() {
      return List.of(new GrpcBackendProvider());
    }

    @Override
    public FqnResolver getFqnResolver() {
      return new HypertraceFqnResolver();
    }
  }
}
