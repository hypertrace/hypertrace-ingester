package org.hypertrace.traceenricher.enrichment.clients;

import static java.util.Collections.emptySet;

import com.typesafe.config.Config;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import io.grpc.Channel;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.attribute.service.client.config.AttributeServiceCachedClientConfig;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.grpcutils.client.GrpcChannelConfig;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;
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
import org.hypertrace.traceenricher.util.UserAgentParser;

public class DefaultClientRegistry implements ClientRegistry {
  private static final String ATTRIBUTE_SERVICE_CONFIG_KEY = "attribute.service.config";
  private static final String ATTRIBUTE_SERVICE_HOST_KEY = ATTRIBUTE_SERVICE_CONFIG_KEY + ".host";
  private static final String ATTRIBUTE_SERVICE_PORT_KEY = ATTRIBUTE_SERVICE_CONFIG_KEY + ".port";
  private static final String CONFIG_SERVICE_HOST_KEY = "config.service.config.host";
  private static final String CONFIG_SERVICE_PORT_KEY = "config.service.config.port";
  private static final String ENTITY_SERVICE_HOST_KEY = "entity.service.config.host";
  private static final String ENTITY_SERVICE_PORT_KEY = "entity.service.config.port";
  private static final String ENTITY_CHANGE_EVENTS_CONFIG_KEY =
      "entity.service.config.change.events.config";
  private static final String ENTITY_CHANGE_EVENTS_CONSUMER_NAME_KEY =
      "entity.service.config.change.events.config.consumer.name";
  private static final String ENTITY_CHANGE_EVENTS_CONSUMER_ENABLED_KEY =
      "entity.service.config.change.events.config.enabled";
  private static final String ENTITY_CHANGE_EVENTS_SCHEMA_REGISTRY_URL_KEY =
      "entity.service.config.change.events.config.schema.registry.url";
  private static final String TRACE_ENTITY_WRITE_THROTTLE_DURATION =
      "trace.entity.write.throttle.duration";
  private static final String TRACE_ENTITY_WRITE_EXCLUDED_ENTITY_TYPES =
      "trace.entity.write.excluded.entity.types";
  private static final String USER_AGENT_PARSER_CONFIG_KEY = "useragent.parser";
  private final Channel configServiceChannel;
  private final Channel entityServiceChannel;
  private final EdsCacheClient edsCacheClient;
  private final EntityDataClient entityDataClient;
  private final EntityCache entityCache;
  private final TraceEntityAccessor entityAccessor;
  private final TraceAttributeReader<StructuredTrace, Event> attributeReader;
  private final GrpcChannelRegistry grpcChannelRegistry;
  private final UserAgentParser userAgentParser;
  private final AttributeServiceCachedClient attributeClient;
  private final Optional<KafkaLiveEventListener<EntityChangeEventKey, EntityChangeEventValue>>
      entityChangeEventListener;

  public DefaultClientRegistry(
      Config config, GrpcChannelRegistry grpcChannelRegistry, Executor cacheLoaderExecutor) {
    this.grpcChannelRegistry = grpcChannelRegistry;

    Channel attributeServiceChannel =
        this.buildChannel(
            config.getString(ATTRIBUTE_SERVICE_HOST_KEY),
            config.getInt(ATTRIBUTE_SERVICE_PORT_KEY));
    this.configServiceChannel =
        this.buildChannel(
            config.getString(CONFIG_SERVICE_HOST_KEY), config.getInt(CONFIG_SERVICE_PORT_KEY));
    this.entityServiceChannel =
        this.buildChannel(
            config.getString(ENTITY_SERVICE_HOST_KEY), config.getInt(ENTITY_SERVICE_PORT_KEY));

    this.attributeClient =
        new AttributeServiceCachedClient(
            attributeServiceChannel,
            AttributeServiceCachedClientConfig.from(
                config.getConfig(ATTRIBUTE_SERVICE_CONFIG_KEY)));
    this.attributeReader = TraceAttributeReaderFactory.build(attributeClient);
    this.edsCacheClient =
        new EdsCacheClient(
            new EntityDataServiceClient(this.entityServiceChannel),
            EntityServiceClientConfig.from(config).getCacheConfig(),
            cacheLoaderExecutor);
    this.entityChangeEventListener =
        getEntityChangeEventConsumer(config, edsCacheClient::updateBasedOnChangeEvent);
    this.entityDataClient = EntityDataClient.builder(this.entityServiceChannel).build();
    this.entityCache = new EntityCache(this.edsCacheClient, cacheLoaderExecutor);
    this.entityAccessor =
        new TraceEntityAccessorBuilder(
                EntityTypeClient.builder(this.entityServiceChannel).build(),
                this.entityDataClient,
                attributeClient)
            .withEntityWriteThrottleDuration(
                config.hasPath(TRACE_ENTITY_WRITE_THROTTLE_DURATION)
                    ? config.getDuration(TRACE_ENTITY_WRITE_THROTTLE_DURATION)
                    : Duration.ofSeconds(15))
            .withExcludeEntityTypes(
                config.hasPath(TRACE_ENTITY_WRITE_EXCLUDED_ENTITY_TYPES)
                    ? Set.copyOf(config.getStringList(TRACE_ENTITY_WRITE_EXCLUDED_ENTITY_TYPES))
                    : emptySet())
            .build();
    this.userAgentParser = new UserAgentParser(config.getConfig(USER_AGENT_PARSER_CONFIG_KEY));
  }

  @Override
  public GrpcChannelRegistry getChannelRegistry() {
    return grpcChannelRegistry;
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
  public UserAgentParser getUserAgentParser() {
    return this.userAgentParser;
  }

  @Override
  public AttributeServiceCachedClient getAttributeClient() {
    return attributeClient;
  }

  public void shutdown() {
    this.grpcChannelRegistry.shutdown();
    this.entityChangeEventListener.ifPresent(
        listener -> {
          try {
            listener.close();
          } catch (Exception e) {

          }
        });
  }

  protected Channel buildChannel(String host, int port) {
    return this.grpcChannelRegistry.forPlaintextAddress(host, port);
  }

  protected Channel buildChannel(String host, int port, GrpcChannelConfig grpcChannelConfig) {
    return this.grpcChannelRegistry.forPlaintextAddress(host, port, grpcChannelConfig);
  }

  private static Optional<KafkaLiveEventListener<EntityChangeEventKey, EntityChangeEventValue>>
      getEntityChangeEventConsumer(
          Config clientsConfig, BiConsumer<EntityChangeEventKey, EntityChangeEventValue> callback) {
    if (clientsConfig.hasPath(ENTITY_CHANGE_EVENTS_CONSUMER_ENABLED_KEY)
        && clientsConfig.getBoolean(ENTITY_CHANGE_EVENTS_CONSUMER_ENABLED_KEY)) {
      String consumerName = clientsConfig.getString(ENTITY_CHANGE_EVENTS_CONSUMER_NAME_KEY);
      Map<String, Object> serdeConfig =
          Collections.singletonMap(
              "schema.registry.url",
              clientsConfig.getString(ENTITY_CHANGE_EVENTS_SCHEMA_REGISTRY_URL_KEY));
      return Optional.of(
          new KafkaLiveEventListener.Builder<EntityChangeEventKey, EntityChangeEventValue>()
              .registerCallback(callback)
              .build(
                  consumerName,
                  clientsConfig.getConfig(ENTITY_CHANGE_EVENTS_CONFIG_KEY),
                  getEntityChangeEventKeySerde(serdeConfig),
                  getEntityChangeEventValueSerde(serdeConfig)));
    }
    return Optional.empty();
  }

  private static Deserializer<EntityChangeEventKey> getEntityChangeEventKeySerde(
      Map<String, Object> serdeConfig) {
    try (KafkaProtobufSerde<EntityChangeEventKey> entityChangeEventKeySerde =
        new KafkaProtobufSerde<>(EntityChangeEventKey.class)) {
      entityChangeEventKeySerde.configure(serdeConfig, true);
      return entityChangeEventKeySerde.deserializer();
    }
  }

  private static Deserializer<EntityChangeEventValue> getEntityChangeEventValueSerde(
      Map<String, Object> serdeConfig) {
    try (Serde<EntityChangeEventValue> entityChangeEventValueSerde =
        new KafkaProtobufSerde<>(EntityChangeEventValue.class)) {
      entityChangeEventValueSerde.configure(serdeConfig, false);
      return entityChangeEventValueSerde.deserializer();
    }
  }
}
