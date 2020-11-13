package org.hypertrace.trace.reader.entities;

import io.grpc.Channel;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;

public class TraceEntityReaderBuilder {
  /**
   * Instantiates a new builder which reuses existing clients
   *
   * @param entityTypeClient
   * @param entityDataClient
   * @param attributeClient
   * @return TraceEntityReaderBuilder
   */
  public static TraceEntityReaderBuilder usingClients(
      EntityTypeClient entityTypeClient,
      EntityDataClient entityDataClient,
      CachingAttributeClient attributeClient) {
    return new TraceEntityReaderBuilder(entityTypeClient, entityDataClient, attributeClient);
  }

  /**
   * Instantiates a new builder which creates default configured caches for connections to each
   * service.
   *
   * @param entityDataChannel
   * @param entityTypeChannel
   * @param attributeChannel
   * @return TraceEntityReaderBuilder
   */
  public static TraceEntityReaderBuilder usingChannels(
      Channel entityDataChannel, Channel entityTypeChannel, Channel attributeChannel) {
    return new TraceEntityReaderBuilder(
        EntityTypeClient.builder(entityTypeChannel).build(),
        EntityDataClient.builder(entityDataChannel).build(),
        CachingAttributeClient.builder(attributeChannel).build());
  }

  private final EntityTypeClient entityTypeClient;
  private final EntityDataClient entityDataClient;
  private final CachingAttributeClient attributeClient;

  private TraceEntityReaderBuilder(
      EntityTypeClient entityTypeClient,
      EntityDataClient entityDataClient,
      CachingAttributeClient attributeClient) {
    this.entityTypeClient = entityTypeClient;
    this.entityDataClient = entityDataClient;
    this.attributeClient = attributeClient;
  }

  public TraceEntityReader build() {
    return new DefaultTraceEntityReader(
        this.entityTypeClient,
        this.entityDataClient,
        this.attributeClient,
        TraceAttributeReader.build(this.attributeClient));
  }
}
