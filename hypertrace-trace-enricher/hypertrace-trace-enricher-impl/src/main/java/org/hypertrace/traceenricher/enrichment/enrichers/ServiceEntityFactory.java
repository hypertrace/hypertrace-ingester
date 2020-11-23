package org.hypertrace.traceenricher.enrichment.enrichers;

import com.google.common.util.concurrent.RateLimiter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.constants.v1.ServiceAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.entity.v1.servicetype.ServiceType;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache.EntityCacheProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for constructing and creating the Service entity
 */
public class ServiceEntityFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceEntityFactory.class);

  private static final RateLimiter MULTIPLE_SERVICES_LIMITER = RateLimiter.create(1 / 60d);

  private static final String FQN_ATTRIBUTE_NAME =
      EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN);
  private static final String SERVICE_TYPE_ATTRIBUTE_NAME =
      EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_SERVICE_TYPE);

  private final EdsClient edsClient;
  private final EntityCache entityCache;

  public ServiceEntityFactory(EdsClient edsClient) {
    this.edsClient = edsClient;
    this.entityCache = EntityCacheProvider.get(edsClient);
  }

  Entity getService(String customerId, String name, String serviceType,
                    Map<String, String> attributes) {
    try {
      List<Entity> services = entityCache.getNameToServiceEntitiesCache()
          .get(Pair.of(customerId, name));
      if (services.size() == 1) {
        return services.get(0);
      } else if (!services.isEmpty()) {
        // Filter out the jaeger service subtypes and see if there is only one matching service.
        List<Entity> nonJaegerServices = services.stream().filter(e -> !isJaegerService(e))
            .collect(Collectors.toList());

        // If there is only one other service which isn't based on Jaeger, pick that since that
        // would be our logical service.
        if (nonJaegerServices.size() == 1) {
          return nonJaegerServices.get(0);
        } else if (nonJaegerServices.size() > 1) {
          if (MULTIPLE_SERVICES_LIMITER.tryAcquire()) {
            LOG.warn("Multiple logical services found with same name; services: {}", services);
          }
          List<Entity> jaegerService = services.stream()
              .filter(this::isJaegerService)
              .collect(Collectors.toList());

          if (jaegerService.size() == 1) {
            return jaegerService.get(0);
          }
        }
      }
    } catch (ExecutionException e) {
      LOG.error("Could not get service; customerId: {}, name: {}", customerId, name, e);
    }

    return createServiceEntity(customerId, name, name, serviceType, attributes);
  }

  @Nonnull
  private org.hypertrace.entity.data.service.v1.Entity createServiceEntity(String customerId, String name,
                                                                           String fqn, String serviceType, Map<String, String> attributes) {
    LOG.info("Creating a new service; customerId: {}, fqn: {}, serviceType: {}",
        customerId, fqn, serviceType);

    org.hypertrace.entity.data.service.v1.Entity.Builder builder =
        org.hypertrace.entity.data.service.v1.Entity.newBuilder()
            .setTenantId(customerId)
            .setEntityType(EntityType.SERVICE.name())
            .setEntityName(name)
            .putIdentifyingAttributes(FQN_ATTRIBUTE_NAME,
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString(fqn)).build());

    attributes.forEach((k, v) -> addStringAttribute(builder, k, v));
    addStringAttribute(builder, SERVICE_TYPE_ATTRIBUTE_NAME, serviceType);
    return edsClient.upsert(builder.build());
  }

  private boolean isJaegerService(Entity service) {
    if (service == null) {
      return false;
    }
    AttributeValue value = service.getAttributesMap().get(SERVICE_TYPE_ATTRIBUTE_NAME);
    return value == null || (value.getTypeCase() == AttributeValue.TypeCase.VALUE &&
        StringUtils.equals(value.getValue().getString(), ServiceType.JAEGER_SERVICE.name()));
  }

  private void addStringAttribute(org.hypertrace.entity.data.service.v1.Entity.Builder builder, String name,
                                  String value) {
    if (StringUtils.isEmpty(value)) {
      return;
    }

    builder.putAttributes(name, org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString(value).build()).build());
  }
}
