package org.hypertrace.traceenricher.enrichment.enrichers.resolver;

import com.google.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.entity.constants.v1.K8sEntityAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.traceenricher.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to resolve the IP address to a proper service name so that the logical services
 * are created based on names rather than IPs even though the host header might be an IP.
 */
public class IpToServiceNameResolver {
  private static final Logger LOG = LoggerFactory.getLogger(IpToServiceNameResolver.class);

  private static final String EXTERNAL_NAME_ATTR =
      Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_EXTERNAL_NAME);
  private static final Set<String> INGRESS_HOST_ATTRIBUTES = ImmutableSet.of(
      Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_LOADBALANCER_INGRESS_HOSTS));
  private static final Set<String> K8S_SERVICE_IP_ATTRIBUTES = ImmutableSet
      .of(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_IP),
          Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_EXTERNAL_IPS),
          Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_LOADBALANCER_IP),
          Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_LOADBALANCER_INGRESS_IPS));

  private final EdsClient edsClient;

  public IpToServiceNameResolver(EdsClient edsClient) {
    this.edsClient = edsClient;
  }

  public String resolveServiceName(InetAddress address, List<Entity> entities) {
    // If the IP is not k8s cluster local IP address, we should check if we can map it to an
    // external DNS name based on the information we have and then resolve the service based on
    // DNS name. If we can't map the IP to a DNS, continue with the external IP as name for now.
    // That behavior also could change in future.
    List<Entity> k8sServices = entities.stream()
        .filter(e -> StringUtils.equals(e.getEntityType(), EntityType.K8S_SERVICE.name()))
        .collect(Collectors.toList());
    if (!k8sServices.isEmpty()) {
      // Check if the IP is same as IP of one of the k8s services linked to the span.
      for (Entity k8sService : k8sServices) {
        org.hypertrace.entity.data.service.v1.Entity service =
            edsClient.getById(k8sService.getCustomerId(), k8sService.getEntityId());

        if (service == null || service.getAttributesCount() == 0) {
          continue;
        }

        Set<String> ips = getStringAttributesFromK8sService(service.getAttributesMap(),
            K8S_SERVICE_IP_ATTRIBUTES);
        if (ips.contains(address.getHostAddress())) {
          // See if the service has an external DNS name that can be used as the service name.
          String externalName = getStringAttributeFromEntity(service.getAttributesMap(),
              EXTERNAL_NAME_ATTR);
          if (externalName != null) {
            return externalName;
          }

          // Check if the k8s service has Ingress Names.
          Set<String> ingressNames = getStringAttributesFromK8sService(service.getAttributesMap(),
              INGRESS_HOST_ATTRIBUTES);
          if (!ingressNames.isEmpty()) {
            // For now, return the first one in the sorted list but we'll need to dig in when this
            // happens.
            // Logging it so that we know more about this case.
            LOG.warn("Multiple ingress hosts present on k8s service. service: {}", service);
            List<String> names = new ArrayList<>(ingressNames);
            Collections.sort(names);
            return names.get(0);
          }

          // By default, return the k8s service name.
          return service.getEntityName();
        }
      }
    }

    // If we are here, that means, we couldn't map the IP to any k8s service.
    // At this point, if the IP is external, use the same as service name and resolve.

    if (!address.isSiteLocalAddress() && !address.isLinkLocalAddress()) {
      return address.getHostAddress();
    }

    // Otherwise, ignore all the private IPs (they could be pod IPs or node IPs) for now.
    return null;
  }

  @Nullable
  private static String getStringAttributeFromEntity(
      Map<String, AttributeValue> map, String attr) {
    org.hypertrace.entity.data.service.v1.AttributeValue value = map.get(attr);
    if (value != null && value.getValue() != null) {
      return value.getValue().getString();
    }

    return null;
  }

  @Nonnull
  private Set<String> getStringAttributesFromK8sService(
      Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> map, Set<String> attributes) {
    Set<String> values = new HashSet<>();
    for (String attr : attributes) {
      org.hypertrace.entity.data.service.v1.AttributeValue value = map.get(attr);
      if (value != null) {
        switch (value.getTypeCase()) {
          case VALUE:
            values.add(value.getValue().getString());
            break;
          case VALUE_LIST:
            values.addAll(
                value.getValueList().getValuesList().stream().map(v -> v.getValue().getString())
                    .collect(Collectors.toList()));
            break;
          default:
            throw new IllegalArgumentException(
                "Unhandled attribute value type: " + value.getTypeCase());
        }
      }
    }
    return values;
  }
}
