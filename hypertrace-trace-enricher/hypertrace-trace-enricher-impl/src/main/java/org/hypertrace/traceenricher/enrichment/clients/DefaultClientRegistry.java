package org.hypertrace.traceenricher.enrichment.clients;

import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executor;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.grpcutils.client.GrpcChannelConfig;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.entity.data.service.client.EdsCacheClient;
import org.hypertrace.entity.data.service.client.EntityDataServiceClient;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceBlockingStub;
import org.hypertrace.entity.service.client.config.EntityServiceClientConfig;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.trace.accessor.entities.TraceEntityAccessor;
import org.hypertrace.trace.accessor.entities.TraceEntityAccessorBuilder;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;
import org.hypertrace.trace.reader.attributes.TraceAttributeReaderFactory;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;

public class DefaultClientRegistry implements ClientRegistry {
  private static final String ATTRIBUTE_SERVICE_HOST_KEY = "attribute.service.config.host";
  private static final String ATTRIBUTE_SERVICE_PORT_KEY = "attribute.service.config.port";
  private static final String CONFIG_SERVICE_HOST_KEY = "config.service.config.host";
  private static final String CONFIG_SERVICE_PORT_KEY = "config.service.config.port";
  private static final String ENTITY_SERVICE_HOST_KEY = "entity.service.config.host";
  private static final String ENTITY_SERVICE_PORT_KEY = "entity.service.config.port";
  private static final String TRACE_ENTITY_WRITE_THROTTLE_DURATION =
      "trace.entity.write.throttle.duration";

  private final ManagedChannel attributeServiceChannel;
  private final ManagedChannel configServiceChannel;
  private final ManagedChannel entityServiceChannel;
  private final EdsCacheClient edsCacheClient;
  private final EntityDataClient entityDataClient;
  private final CachingAttributeClient cachingAttributeClient;
  private final EntityCache entityCache;
  private final TraceEntityAccessor entityAccessor;
  private final TraceAttributeReader<StructuredTrace, Event> attributeReader;
  private final GrpcChannelRegistry grpcChannelRegistry = new GrpcChannelRegistry();

  public DefaultClientRegistry(Config config, Executor cacheLoaderExecutor) {
    this.attributeServiceChannel =
        this.buildChannel(
            config.getString(ATTRIBUTE_SERVICE_HOST_KEY),
            config.getInt(ATTRIBUTE_SERVICE_PORT_KEY));
    this.configServiceChannel =
        this.buildChannel(
            config.getString(CONFIG_SERVICE_HOST_KEY), config.getInt(CONFIG_SERVICE_PORT_KEY));
    this.entityServiceChannel =
        this.buildChannel(
            config.getString(ENTITY_SERVICE_HOST_KEY), config.getInt(ENTITY_SERVICE_PORT_KEY));

    this.cachingAttributeClient =
        CachingAttributeClient.builder(this.attributeServiceChannel)
            .withMaximumCacheContexts(100) // 100 Tenants
            .withCacheExpiration(Duration.of(15, ChronoUnit.MINUTES))
            .build();

    this.attributeReader = TraceAttributeReaderFactory.build(this.cachingAttributeClient);
    this.edsCacheClient =
        new EdsCacheClient(
            new EntityDataServiceClient(this.entityServiceChannel),
            EntityServiceClientConfig.from(config).getCacheConfig(),
            cacheLoaderExecutor);
    this.entityDataClient = EntityDataClient.builder(this.entityServiceChannel).build();
    this.entityCache = new EntityCache(this.edsCacheClient, cacheLoaderExecutor);
    this.entityAccessor =
        new TraceEntityAccessorBuilder(
                EntityTypeClient.builder(this.entityServiceChannel).build(),
                this.entityDataClient,
                this.cachingAttributeClient)
            .withEntityWriteThrottleDuration(
                config.hasPath(TRACE_ENTITY_WRITE_THROTTLE_DURATION)
                    ? config.getDuration(TRACE_ENTITY_WRITE_THROTTLE_DURATION)
                    : Duration.ofSeconds(15))
            .build();
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
  public TraceEntityAccessor getTraceEntityAccessor() {
    return this.entityAccessor;
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
  public EntityDataClient getEntityDataClient() {
    return this.entityDataClient;
  }

  @Override
  public EntityQueryServiceBlockingStub getEntityQueryServiceClient() {
    return EntityQueryServiceGrpc.newBlockingStub(entityServiceChannel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
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
    this.grpcChannelRegistry.shutdown();
  }

  protected ManagedChannel buildChannel(String host, int port) {
    return this.grpcChannelRegistry.forPlaintextAddress(host, port);
  }

  protected ManagedChannel buildChannel(
      String host, int port, GrpcChannelConfig grpcChannelConfig) {
    return this.grpcChannelRegistry.forPlaintextAddress(host, port, grpcChannelConfig);
  }
}
