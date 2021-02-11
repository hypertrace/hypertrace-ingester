package org.hypertrace.trace.reader.entities;

import io.grpc.Channel;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;

public class TraceEntityClientContext {
  /**
   * Instantiates a new builder which reuses existing clients
   *
   * @param entityTypeClient
   * @param entityDataClient
   * @param attributeClient
   * @return {@link TraceEntityClientContext}
   */
  public static TraceEntityClientContext usingClients(
      EntityTypeClient entityTypeClient,
      EntityDataClient entityDataClient,
      CachingAttributeClient attributeClient) {
    return new TraceEntityClientContext(entityTypeClient, entityDataClient, attributeClient);
  }

  /**
   * Instantiates a new builder which creates default configured caches for connections to each
   * service.
   *
   * @param entityDataChannel
   * @param entityTypeChannel
   * @param attributeChannel
   * @return {@link TraceEntityClientContext}
   */
  public static TraceEntityClientContext usingChannels(
      Channel entityDataChannel, Channel entityTypeChannel, Channel attributeChannel) {
    return new TraceEntityClientContext(
        EntityTypeClient.builder(entityTypeChannel).build(),
        EntityDataClient.builder(entityDataChannel).build(),
        CachingAttributeClient.builder(attributeChannel).build());
  }

  private final EntityTypeClient entityTypeClient;
  private final EntityDataClient entityDataClient;
  private final CachingAttributeClient attributeClient;

  private TraceEntityClientContext(
      EntityTypeClient entityTypeClient,
      EntityDataClient entityDataClient,
      CachingAttributeClient attributeClient) {
    this.entityTypeClient = entityTypeClient;
    this.entityDataClient = entityDataClient;
    this.attributeClient = attributeClient;
  }

  public EntityTypeClient getEntityTypeClient() {
    return entityTypeClient;
  }

  public EntityDataClient getEntityDataClient() {
    return entityDataClient;
  }

  public CachingAttributeClient getAttributeClient() {
    return attributeClient;
  }
}
