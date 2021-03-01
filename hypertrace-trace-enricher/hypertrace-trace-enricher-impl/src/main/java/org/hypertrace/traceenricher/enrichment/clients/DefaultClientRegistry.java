package org.hypertrace.traceenricher.enrichment.clients;

import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.data.service.client.EdsCacheClient;
import org.hypertrace.entity.data.service.client.EntityDataServiceClient;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.service.client.config.EntityServiceClientConfig;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;
import org.hypertrace.trace.reader.attributes.TraceAttributeReaderFactory;
import org.hypertrace.trace.reader.entities.TraceEntityClientContext;
import org.hypertrace.trace.reader.entities.TraceEntityReader;
import org.hypertrace.trace.reader.entities.TraceEntityReaderFactory;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;

public class DefaultClientRegistry implements ClientRegistry {
  private static final String ATTRIBUTE_SERVICE_HOST_KEY = "attribute.service.config.host";
  private static final String ATTRIBUTE_SERVICE_PORT_KEY = "attribute.service.config.port";
  private static final String CONFIG_SERVICE_HOST_KEY = "config.service.config.host";
  private static final String CONFIG_SERVICE_PORT_KEY = "config.service.config.port";
  private static final String ENTITY_SERVICE_HOST_KEY = "entity.service.config.host";
  private static final String ENTITY_SERVICE_PORT_KEY = "entity.service.config.port";

  private final ManagedChannel attributeServiceChannel;
  private final ManagedChannel configServiceChannel;
  private final ManagedChannel entityServiceChannel;
  private final EdsCacheClient edsCacheClient;
  private final CachingAttributeClient cachingAttributeClient;
  private final EntityCache entityCache;
  private final TraceEntityReader<StructuredTrace, Event> entityReader;
  private final TraceAttributeReader<StructuredTrace, Event> attributeReader;

  public DefaultClientRegistry(Config config) {
    this.attributeServiceChannel =
        this.buildChannel(config, ATTRIBUTE_SERVICE_HOST_KEY, ATTRIBUTE_SERVICE_PORT_KEY);
    this.configServiceChannel =
        this.buildChannel(config, CONFIG_SERVICE_HOST_KEY, CONFIG_SERVICE_PORT_KEY);
    this.entityServiceChannel =
        this.buildChannel(config, ENTITY_SERVICE_HOST_KEY, ENTITY_SERVICE_PORT_KEY);

    this.cachingAttributeClient =
        CachingAttributeClient.builder(this.attributeServiceChannel)
            .withMaximumCacheContexts(100) // 100 Tenants
            .withCacheExpiration(Duration.of(15, ChronoUnit.MINUTES))
            .build();

    this.attributeReader = TraceAttributeReaderFactory.build(this.cachingAttributeClient);
    this.edsCacheClient =
        new EdsCacheClient(
            new EntityDataServiceClient(this.entityServiceChannel),
            EntityServiceClientConfig.from(config).getCacheConfig());
    this.entityCache = new EntityCache(this.edsCacheClient);
    this.entityReader =
        TraceEntityReaderFactory.build(
            TraceEntityClientContext.usingClients(
                EntityTypeClient.builder(this.entityServiceChannel).build(),
                EntityDataClient.builder(this.entityServiceChannel).build(),
                this.cachingAttributeClient));
  }

  @Override
  public Channel getAttributeServiceChannel() {
    return this.attributeServiceChannel;
  }

  @Override
  public Channel getEntityServiceChannel() {
    return this.entityServiceChannel;
  }

  @Override
  public Channel getConfigServiceChannel() {
    return this.configServiceChannel;
  }

  @Override
  public TraceEntityReader<StructuredTrace, Event> getEntityReader() {
    return this.entityReader;
  }

  @Override
  public TraceAttributeReader<StructuredTrace, Event> getAttributeReader() {
    return this.attributeReader;
  }

  @Override
  public EdsCacheClient getEdsCacheClient() {
    return this.edsCacheClient;
  }

  @Override
  public EntityCache getEntityCache() {
    return this.entityCache;
  }

  @Override
  public CachingAttributeClient getCachingAttributeClient() {
    return this.cachingAttributeClient;
  }

  public void shutdown() {
    this.attributeServiceChannel.shutdown();
    this.configServiceChannel.shutdown();
    this.entityServiceChannel.shutdown();
  }

  private ManagedChannel buildChannel(Config config, String hostKey, String portKey) {
    return ManagedChannelBuilder.forAddress(config.getString(hostKey), config.getInt(portKey))
        .usePlaintext()
        .build();
  }
}
