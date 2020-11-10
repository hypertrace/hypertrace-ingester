package org.hypertrace.traceenricher.enrichment.enrichers.resolver;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.entity.constants.v1.K8sEntityAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IpToServiceNameResolverTest {
  private static final String TENANT_ID = "test_tenant";
  private static EdsClient edsClient;
  private static IpToServiceNameResolver ipResolver;

  @BeforeAll
  public static void setUpClass() {
    edsClient = mock(EdsClient.class);
    ipResolver = new IpToServiceNameResolver(edsClient);
  }

  @BeforeEach
  public void setup() {
    reset(edsClient);
  }

  @Test
  public void testPublicIpResolution() {
    String ip = "35.233.230.80";
    InetAddress address = InetAddresses.forString(ip);

    // No related k8s services so nothing much to map to k8s.
    String name = ipResolver.resolveServiceName(address, Collections.emptyList());
    Assertions.assertEquals(ip, name);

    // Non k8s service entity and nothing should change.
    Entity podEntity = getEntity(EntityType.K8S_POD.name());
    name = ipResolver.resolveServiceName(address, Collections.singletonList(podEntity));
    Assertions.assertEquals(ip, name);

    // Pass a k8s entity now, which doesn't have this IP.
    Entity k8sService1 = getEntity(EntityType.K8S_SERVICE.name());
    when(edsClient.getById(eq(TENANT_ID), eq(k8sService1.getEntityId())))
        .thenReturn(getK8sEntity(k8sService1, Map.of()));
    name = ipResolver.resolveServiceName(address, ImmutableList.of(podEntity, k8sService1));
    Assertions.assertEquals(ip, name);

    // A k8s service which has the given IP as clusterIP. However, this service doesn't
    // have an external name, so the service name is same as k8s entity name.
    Entity k8sService2 = getEntity(EntityType.K8S_SERVICE.name());
    when(edsClient.getById(eq(TENANT_ID), eq(k8sService2.getEntityId())))
        .thenReturn(getK8sEntity(k8sService2,
            Map.of(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_IP),
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString(ip)).build())
        ));
    name = ipResolver.resolveServiceName(address, ImmutableList.of(podEntity, k8sService1, k8sService2));
    Assertions.assertEquals(k8sService2.getEntityName(), name);

    // Finally a k8s service which has the given IP as clusterIP and also has an external name.
    Entity k8sService3 = getEntity(EntityType.K8S_SERVICE.name());
    String externalName = "myService";
    when(edsClient.getById(eq(TENANT_ID), eq(k8sService3.getEntityId())))
        .thenReturn(getK8sEntity(k8sService3,
            Map.of(
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_IP),
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString(ip)).build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_EXTERNAL_NAME),
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString(externalName)).build()
            )
        ));
    name = ipResolver.resolveServiceName(address, ImmutableList.of(podEntity, k8sService1, k8sService3));
    Assertions.assertEquals(externalName, name);
  }

  @Test
  public void testPrivateIpResolution() {
    String ip = "10.3.240.237";
    InetAddress address = InetAddresses.forString(ip);

    // No related k8s services so nothing much to map to k8s.
    String name = ipResolver.resolveServiceName(address, Collections.emptyList());
    Assertions.assertNull(name);

    // Non k8s service entity and nothing should change.
    Entity podEntity = getEntity(EntityType.K8S_POD.name());
    name = ipResolver.resolveServiceName(address, Collections.singletonList(podEntity));
    Assertions.assertNull(name);

    // Pass a k8s entity now, which doesn't have this IP.
    Entity k8sService1 = getEntity(EntityType.K8S_SERVICE.name());
    when(edsClient.getById(eq(TENANT_ID), eq(k8sService1.getEntityId())))
        .thenReturn(getK8sEntity(k8sService1, Map.of()));
    name = ipResolver.resolveServiceName(address, ImmutableList.of(podEntity, k8sService1));
    Assertions.assertNull(name);

    // A k8s service which has the given IP as clusterIP. However, this service doesn't
    // have an external name, so the service name is same as k8s entity name.
    Entity k8sService2 = getEntity(EntityType.K8S_SERVICE.name());
    when(edsClient.getById(eq(TENANT_ID), eq(k8sService2.getEntityId())))
        .thenReturn(getK8sEntity(k8sService2,
            Map.of(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_IP),
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString(ip)).build())
        ));
    name = ipResolver.resolveServiceName(address, ImmutableList.of(podEntity, k8sService1, k8sService2));
    Assertions.assertEquals(k8sService2.getEntityName(), name);

    // Finally a k8s service which has the given IP as clusterIP and also has an external name.
    Entity k8sService3 = getEntity(EntityType.K8S_SERVICE.name());
    String externalName = "myService";
    when(edsClient.getById(eq(TENANT_ID), eq(k8sService3.getEntityId())))
        .thenReturn(getK8sEntity(k8sService3,
            Map.of(
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_IP),
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString(ip)).build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_EXTERNAL_NAME),
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString(externalName)).build()
            )
        ));
    name = ipResolver.resolveServiceName(address, ImmutableList.of(podEntity, k8sService1, k8sService3));
    Assertions.assertEquals(externalName, name);
  }

  private Entity getEntity(String entityType) {
    return Entity.newBuilder().setEntityType(entityType).setCustomerId(TENANT_ID)
        .setEntityName(UUID.randomUUID().toString())
        .setEntityId(UUID.randomUUID().toString()).build();
  }

  private org.hypertrace.entity.data.service.v1.Entity getK8sEntity(Entity k8sService,
                                                                    Map<String, AttributeValue> attributes) {
    return org.hypertrace.entity.data.service.v1.Entity.newBuilder()
        .setTenantId(k8sService.getCustomerId())
        .setEntityId(k8sService.getEntityId())
        .setEntityName(k8sService.getEntityName())
        .setEntityType(k8sService.getEntityType())
        .putAllAttributes(attributes).build();
  }
}
