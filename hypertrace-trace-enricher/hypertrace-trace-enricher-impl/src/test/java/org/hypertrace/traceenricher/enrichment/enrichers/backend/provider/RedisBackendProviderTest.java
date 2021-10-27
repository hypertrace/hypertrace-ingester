package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
import static org.hypertrace.traceenricher.TestUtil.buildAttributeValue;
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
import org.hypertrace.core.semantic.convention.constants.db.OTelDbSemanticConventions;
import org.hypertrace.core.semantic.convention.constants.span.OTelSpanSemanticConventions;
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

public class RedisBackendProviderTest {
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
  public void checkBackendEntityGeneratedFromRedisEvent() {
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
                            "redis.connection",
                            AttributeValue.newBuilder().setValue("redis-cart:6379").build(),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "redis.command",
                            AttributeValue.newBuilder().setValue("GET").build(),
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
                            AttributeValue.newBuilder().setValue("0").build(),
                            "redis.args",
                            AttributeValue.newBuilder()
                                .setValue("key<product_5d644175551847d7408760b3>")
                                .build()))
                    .build())
            .setEventName("reactive.redis.exit")
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
    BackendInfo backendInfo =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get();
    final Entity backendEntity = backendInfo.getEntity();
    assertEquals("redis-cart:6379", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue()
            .getString(),
        "REDIS");
    assertEquals(
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST))
            .getValue()
            .getString(),
        "redis-cart");
    assertEquals(
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT))
            .getValue()
            .getString(),
        "6379");
    assertEquals(
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT))
            .getValue()
            .getString(),
        "reactive.redis.exit");
    assertEquals(
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID))
            .getValue()
            .getString(),
        "62646630336466616266356337306638");
    assertEquals(
        backendEntity.getAttributesMap().get("redis.command").getValue().getString(), "GET");
    assertEquals(
        backendEntity.getAttributesMap().get("redis.args").getValue().getString(),
        "key<product_5d644175551847d7408760b3>");
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(Map.of("BACKEND_OPERATION", AttributeValueCreator.create("GET")), attributes);
  }

  @Test
  public void checkBackendEntityGeneratedFromRedisEventOtelFormat() {
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
                            OTelDbSemanticConventions.DB_SYSTEM.getValue(),
                            buildAttributeValue(
                                OTelDbSemanticConventions.REDIS_DB_SYSTEM_VALUE.getValue()),
                            OTelDbSemanticConventions.DB_CONNECTION_STRING.getValue(),
                            buildAttributeValue("redis-cart:6379"),
                            OTelSpanSemanticConventions.NET_PEER_NAME.getValue(),
                            buildAttributeValue("redis-cart"),
                            OTelSpanSemanticConventions.NET_PEER_PORT.getValue(),
                            buildAttributeValue("6379"),
                            "span.kind",
                            AttributeValue.newBuilder().setValue("client").build(),
                            "k8s.pod_id",
                            buildAttributeValue("55636196-c840-11e9-a417-42010a8a0064"),
                            "db.operation",
                            AttributeValue.newBuilder().setValue("GET").build(),
                            "db.redis.database_index",
                            AttributeValue.newBuilder().setValue("15").build(),
                            "docker.container_id",
                            buildAttributeValue(
                                "ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f"),
                            "FLAGS",
                            buildAttributeValue("0")))
                    .build())
            .setEventName("reactive.redis.exit")
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
    BackendInfo backendInfo =
        backendEntityEnricher.resolve(e, structuredTrace, structuredTraceGraph).get();
    final Entity backendEntity = backendInfo.getEntity();
    assertEquals("redis-cart:6379", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue()
            .getString(),
        "REDIS");
    assertEquals(
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST))
            .getValue()
            .getString(),
        "redis-cart");
    assertEquals(
        backendEntity
            .getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT))
            .getValue()
            .getString(),
        "6379");
    assertEquals(
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT))
            .getValue()
            .getString(),
        "reactive.redis.exit");
    assertEquals(
        backendEntity
            .getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID))
            .getValue()
            .getString(),
        "62646630336466616266356337306638");
    Map<String, AttributeValue> attributes = backendInfo.getAttributes();
    assertEquals(
        Map.of(
            "BACKEND_OPERATION",
            AttributeValueCreator.create("GET"),
            "BACKEND_DESTINATION",
            AttributeValueCreator.create("15")),
        attributes);
  }

  static class MockBackendEntityEnricher extends AbstractBackendEntityEnricher {

    @Override
    public void setup(Config enricherConfig, ClientRegistry clientRegistry) {}

    @Override
    public List<BackendProvider> getBackendProviders() {
      return List.of(new RedisBackendProvider());
    }

    @Override
    public FqnResolver getFqnResolver() {
      return new HypertraceFqnResolver();
    }
  }
}
