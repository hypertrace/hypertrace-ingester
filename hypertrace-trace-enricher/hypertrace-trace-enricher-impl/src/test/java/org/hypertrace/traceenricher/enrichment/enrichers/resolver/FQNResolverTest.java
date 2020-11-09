package org.hypertrace.traceenricher.enrichment.enrichers.resolver;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.constants.v1.K8sEntityAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FQNResolverTest {
  private static final String TEST_TENANT_ID = "testTenantId";

  private EdsClient edsClient;
  private FQNResolver fqnResolver;

  @BeforeEach
  public void setup() {
    EntityCache.EntityCacheProvider.clear();
    edsClient = mock(EdsClient.class);
    fqnResolver = new FQNResolver(edsClient);
  }

  @AfterEach
  public void teardown() {
    EntityCache.EntityCacheProvider.clear();
  }

  @Test
  public void testFQNResolveNoNamespaceCluster() {
    String fqn = fqnResolver.resolve("loginservice", createMockEvent());
    Assertions.assertEquals("loginservice", fqn);
  }

  @Test
  public void testFQNResolveSvcClusterLocal() {
    Event event = createMockEvent();
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
        AttributeValueCreator.create("mypastryshop"));
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
        AttributeValueCreator.create("dev"));
    String fqn = fqnResolver.resolve("loginservice.svc.cluster.local", event);
    Assertions.assertEquals("loginservice.mypastryshop.dev", fqn);
  }

  @Test
  public void testFQNResolveSameNamespaceCluster() {
    Event event = createMockEvent();
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
        AttributeValueCreator.create("mypastryshop"));
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
        AttributeValueCreator.create("dev"));
    String fqn = fqnResolver.resolve("loginservice", event);
    Assertions.assertEquals("loginservice.mypastryshop.dev", fqn);
  }

  @Test
  public void testResolveHostnameWithNamespace() {
    when(edsClient.getEntitiesByName(
        eq(TEST_TENANT_ID), eq(EntityType.K8S_NAMESPACE.name()), eq("mycookiesshop")))
        .thenReturn(List.of(mockNamespaceEntity("dev")));
    Event event = createMockEvent();
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
        AttributeValueCreator.create("mypastryshop"));
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
        AttributeValueCreator.create("dev"));
    String fqn = fqnResolver.resolve("loginservice.mycookiesshop", event);
    Assertions.assertEquals("loginservice.mycookiesshop.dev", fqn);
  }

  @Test
  public void testResolveHostnameWithNamespaceDifferentCluster() {
    when(edsClient.getEntitiesByName(
        eq(TEST_TENANT_ID), eq(EntityType.K8S_NAMESPACE.name()), eq("myfineartshop")))
        .thenReturn(List.of(mockNamespaceEntity("prod")));
    Event event = createMockEvent();
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
        AttributeValueCreator.create("mypastryshop"));
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
        AttributeValueCreator.create("dev"));
    String fqn = fqnResolver.resolve("loginservice.myfineartshop", event);
    Assertions.assertEquals("loginservice.myfineartshop", fqn);
  }

  @Test
  public void testFQNResolveDifferentNamespaceInvalid() {
    Event event = createMockEvent();
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
        AttributeValueCreator.create("mypastryshop"));
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
        AttributeValueCreator.create("dev"));
    String fqn = fqnResolver.resolve("loginservice.myfineartshop", event);
    Assertions.assertEquals("loginservice.myfineartshop", fqn);
  }

  @Test
  public void testFQNResolveValidDNS() {
    Event event = createMockEvent();
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
        AttributeValueCreator.create("mypastryshop"));
    event.getAttributes().getAttributeMap().put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
        AttributeValueCreator.create("dev"));
    String fqn = fqnResolver.resolve("abc.xyz.com", event);
    Assertions.assertEquals("abc.xyz.com", fqn);
  }

  private Entity mockNamespaceEntity(String clusterName) {
    return Entity.newBuilder()
        .setEntityId(UUID.randomUUID().toString())
        .putAttributes(
            Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
            AttributeValue.newBuilder()
                .setValue(Value.newBuilder().setString(clusterName).build())
                .build())
        .build();
  }

  private Event createMockEvent() {
    Event e = mock(Event.class);
    when((e.getCustomerId())).thenReturn(TEST_TENANT_ID);
    when(e.getAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());
    when(e.getEnrichedAttributes())
        .thenReturn(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build());
    lenient().when(e.getMetrics())
        .thenReturn(Metrics.newBuilder().setMetricMap(new HashMap<>()).build());
    return e;
  }
}
