package org.hypertrace.traceenricher.enrichment.enrichers.endpoint;

import com.google.common.base.Preconditions;
import com.google.protobuf.TextFormat;
import org.hypertrace.entity.constants.v1.ApiAttribute;
import org.hypertrace.entity.constants.v1.ServiceAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.client.EntityDataServiceClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dao wrapper on top of {@link EntityDataServiceClient} to create or access API entities easily.
 */
public class ApiEntityDao {

  private static Logger LOGGER = LoggerFactory.getLogger(ApiEntityDao.class);

  private static final String DISCOVERED_FROM = "OPERATION_NAME";
  private static final String DISCOVERED_STATE = "DISCOVERED";
  static final String API_TYPE = "OPERATION_NAME";

  private static final String API_DISCOVERY_FROM_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_DISCOVERY_FROM);
  private static final String API_DISCOVERY_STATE_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_DISCOVERY_STATE);
  private static final String API_NAME_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_NAME);

  private static final String SERVICE_ID_ATTR =
      EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_ID);
  private static final String SERVICE_NAME_ATTR =
      EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_NAME);
  private static final String API_URL_PATTERN_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_URL_PATTERN);
  private static final String API_TYPE_ATTR =
      EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_API_TYPE);

  private final EdsClient edsClient;

  public ApiEntityDao(EdsClient client) {
    edsClient = client;
  }

  public Entity upsertApiEntity(
      String tenantId, String serviceId, String serviceName, String apiType, String apiName) {

    Preconditions.checkNotNull(tenantId, "tenantId can't be empty");
    Preconditions.checkNotNull(serviceId, "serviceId can't be empty");
    Preconditions.checkNotNull(serviceName, "serviceName can't be empty");
    Preconditions.checkNotNull(apiType, "apiType can't be empty");
    Preconditions.checkNotNull(apiName, "apiName can't be empty");

    String pattern = apiName;

    Entity.Builder entityBuilder =
        Entity.newBuilder()
            .setTenantId(tenantId)
            .setEntityType(EntityType.API.name())
            .setEntityName(pattern)
            .putIdentifyingAttributes(API_URL_PATTERN_ATTR, createAttributeValue(pattern))
            .putIdentifyingAttributes(SERVICE_ID_ATTR, createAttributeValue(serviceId))
            .putIdentifyingAttributes(API_TYPE_ATTR, createAttributeValue(apiType))
            .putAttributes(SERVICE_NAME_ATTR, createAttributeValue(serviceName))
            .putAttributes(API_DISCOVERY_FROM_ATTR, createAttributeValue(DISCOVERED_FROM))
            .putAttributes(API_DISCOVERY_STATE_ATTR, createAttributeValue(DISCOVERED_STATE))
            .putAttributes(API_NAME_ATTR, createAttributeValue(apiName));

    Entity entity = entityBuilder.build();
    LOGGER.info("Upserting Api entity: [{}]", TextFormat.shortDebugString(entity));
    return edsClient.upsert(entity);
  }

  private AttributeValue createAttributeValue(String value) {
    return AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString(value).build())
        .build();
  }
}
