package org.hypertrace.semantic.convention.utils.span;

import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.spannormalizer.SpanIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

public class SpanStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpanStore.class);
  private static final String SPAN_STORE_ENABLED = "span.store.enabled";
  private static final String SPAN_STORE_REDIS_SENTINEL_MODE = "span.store.redis.sentinelMode";
  private static final String SPAN_STORE_REDIS_SENTINEL_MASTER = "span.store.redis.sentinelMaster";
  private static final String SPAN_STORE_REDIS_SENTINEL_HOST = "span.store.redis.sentinelHost";
  private static final String SPAN_STORE_REDIS_SENTINEL_PORT = "span.store.redis.sentinelPort";
  private static final String SPAN_STORE_REDIS_HOST = "span.store.redis.host";
  private static final String SPAN_STORE_REDIS_PORT = "span.store.redis.port";
  private static final String SPAN_STORE_REDIS_USER = "span.store.redis.user";
  private static final String SPAN_STORE_REDIS_PASSWORD = "span.store.redis.password";

  private static final ByteBuffer TRACE_ID = ByteBuffer.wrap("t".getBytes());

  private static final String IS_TRIM = "is_trim";
  private static final AttributeValue ATTRIBUTE_VALUE_FALSE =
      AttributeValue.newBuilder().setValue("false").build();
  private static final AttributeValue ATTRIBUTE_VALUE_TRUE =
      AttributeValue.newBuilder().setValue("true").build();
  private static final String SPAN_BYPASSED_CONFIG = "processor.bypass.key";
  private static final Duration KEY_TIMEOUT = Duration.ofMinutes(5);

  private Optional<JedisProvider> jedisProvider;
  private Optional<String> bypassKey;
  private Counter counter;
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  public SpanStore(Config config) {
    boolean enabled =
        config.hasPath(SPAN_STORE_ENABLED) ? config.getBoolean(SPAN_STORE_ENABLED) : false;
    if (enabled) {
      try {
        jedisProvider = Optional.of(new JedisProviderFactory().getJedisProvider(config));
      } catch (Exception ex) {
        LOGGER.error("Failed to create jedis provider", ex);
        jedisProvider = Optional.empty();
      }
      bypassKey =
          config.hasPath(SPAN_BYPASSED_CONFIG)
              ? Optional.of(config.getString(SPAN_BYPASSED_CONFIG))
              : Optional.empty();
    } else {
      jedisProvider = Optional.empty();
    }
    counter = new Counter();
    scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
    scheduledThreadPoolExecutor.scheduleAtFixedRate(this::logCounter, 1, 1, TimeUnit.MINUTES);
    LOGGER.info("Initialized span store with redis client {}", jedisProvider);
  }

  public Event pushToSpanStoreAndTrimSpan(Event event) {
    counter.totalPushCallCount.incrementAndGet();
    if (jedisProvider.isPresent()) {
      try {
        counter.totalPushCount.incrementAndGet();
        SpanIdentity spanIdentity =
            SpanIdentity.newBuilder()
                .setTenantId(event.getCustomerId())
                .setTraceId(TRACE_ID)
                .setSpanId(event.getEventId())
                .build();
        Event eventCopy = Event.newBuilder(event).build();
        jedisProvider
            .get()
            .put(spanIdentity.toByteBuffer().array(), eventCopy.toByteBuffer().array());
        counter.totalPushSuccessCount.incrementAndGet();
        Optional<AttributeValue> attributeValue =
            bypassKey.flatMap(
                key -> Optional.ofNullable(event.getAttributes().getAttributeMap().get(key)));
        Map<String, AttributeValue> attributesMap =
            attributeValue
                .map(value -> Map.of(bypassKey.get(), value))
                .orElse(Collections.emptyMap());
        return Event.newBuilder(event)
            .setAttributes(Attributes.newBuilder().setAttributeMap(attributesMap).build())
            .setEnrichedAttributes(
                Attributes.newBuilder()
                    .setAttributeMap(Map.of(IS_TRIM, ATTRIBUTE_VALUE_TRUE))
                    .build())
            .build();
      } catch (IOException e) {
        counter.totalPushErrorCount.incrementAndGet();
        LOGGER.error("Unable to push event to span store", e);
      }
    }
    return event;
  }

  public Event retrieveFromSpanStoreAndFillSpan(Event event) {
    counter.totalGetCallCount.incrementAndGet();
    if (jedisProvider.isPresent()
        && event.getEnrichedAttributes() != null
        && event.getEnrichedAttributes().getAttributeMap() != null
        && !event.getEnrichedAttributes().getAttributeMap().isEmpty()) {
      if (event
          .getEnrichedAttributes()
          .getAttributeMap()
          .getOrDefault(IS_TRIM, ATTRIBUTE_VALUE_FALSE)
          .equals(ATTRIBUTE_VALUE_TRUE)) {
        try {
          counter.totalGetCount.incrementAndGet();
          SpanIdentity spanIdentity =
              SpanIdentity.newBuilder()
                  .setTenantId(event.getCustomerId())
                  .setTraceId(TRACE_ID)
                  .setSpanId(event.getEventId())
                  .build();
          byte[] bytes = jedisProvider.get().get(spanIdentity.toByteBuffer().array());
          if (bytes != null) {
            counter.totalGetSuccessCount.incrementAndGet();
            return Event.fromByteBuffer(ByteBuffer.wrap(bytes));
          } else {
            counter.totalGetMissCount.incrementAndGet();
            LOGGER.error(
                "Null result when retrieving span with id {} from span store", spanIdentity);
            throw new RuntimeException("Null result when retrieving span with id");
          }
        } catch (Exception e) {
          counter.totalGetErrorCount.incrementAndGet();
          LOGGER.error("Unable to retrieve event from span store", e);
          throw new RuntimeException("Unable to retrieve event from span store", e);
        }
      }
    }
    return event;
  }

  private void logCounter() {
    Counter currentCounter = counter;
    counter = new Counter();
    LOGGER.info(currentCounter.toString());
  }

  class Counter {
    AtomicLong totalPushCallCount = new AtomicLong();
    AtomicLong totalPushCount = new AtomicLong();
    AtomicLong totalPushSuccessCount = new AtomicLong();
    AtomicLong totalPushErrorCount = new AtomicLong();

    AtomicLong totalGetCallCount = new AtomicLong();
    AtomicLong totalGetCount = new AtomicLong();
    AtomicLong totalGetSuccessCount = new AtomicLong();
    AtomicLong totalGetMissCount = new AtomicLong();
    AtomicLong totalGetErrorCount = new AtomicLong();

    @Override
    public String toString() {
      return "Counter{"
          + "totalPushCallCount="
          + totalPushCallCount
          + ", totalPushCount="
          + totalPushCount
          + ", totalPushSuccessCount="
          + totalPushSuccessCount
          + ", totalPushErrorCount="
          + totalPushErrorCount
          + ", totalGetCallCount="
          + totalGetCallCount
          + ", totalGetCount="
          + totalGetCount
          + ", totalGetSuccessCount="
          + totalGetSuccessCount
          + ", totalGetMissCount="
          + totalGetMissCount
          + ", totalGetErrorCount="
          + totalGetErrorCount
          + '}';
    }
  }

  interface JedisProvider {
    void put(byte[] key, byte[] value);

    byte[] get(byte[] key);
  }

  class JedisProviderFactory {
    JedisProvider getJedisProvider(Config config) {
      boolean sentinelMode = config.getBoolean(SPAN_STORE_REDIS_SENTINEL_MODE);
      if (sentinelMode) {
        return new JedisSentinelPoolWrapper(config);
      } else {
        return new JedisPoolWrapper(config);
      }
    }
  }

  class JedisSentinelPoolWrapper implements JedisProvider {
    private JedisSentinelPool jedisSentinelPool;

    JedisSentinelPoolWrapper(Config config) {
      Set<String> sentinels = new HashSet<>();
      String host = config.getString(SPAN_STORE_REDIS_SENTINEL_HOST);
      int port = config.getInt(SPAN_STORE_REDIS_SENTINEL_PORT);
      String master = config.getString(SPAN_STORE_REDIS_SENTINEL_MASTER);
      sentinels.add(new HostAndPort(host, port).toString());
      try {
        jedisSentinelPool = new JedisSentinelPool(master, sentinels);
      } catch (Exception ex) {
        LOGGER.error(
            "Failed to create jedis sentinel pool with host {}, port {} and master {}",
            host,
            port,
            master,
            ex);
        throw ex;
      }
    }

    @Override
    public void put(byte[] key, byte[] value) {
      try (Jedis jedis = jedisSentinelPool.getResource()) {
        jedis.setex(key, KEY_TIMEOUT.getSeconds(), value);
      }
    }

    @Override
    public byte[] get(byte[] key) {
      try (Jedis jedis = jedisSentinelPool.getResource()) {
        return jedis.get(key);
      }
    }
  }

  class JedisPoolWrapper implements JedisProvider {
    private JedisPool jedisPool;

    JedisPoolWrapper(Config config) {
      final JedisPoolConfig poolConfig = getJedisPoolConfig();
      jedisPool =
          new JedisPool(
              poolConfig,
              config.getString(SPAN_STORE_REDIS_HOST),
              config.getInt(SPAN_STORE_REDIS_PORT));
    }

    private JedisPoolConfig getJedisPoolConfig() {
      final JedisPoolConfig poolConfig = new JedisPoolConfig();
      poolConfig.setMaxTotal(32);
      poolConfig.setMaxIdle(16);
      poolConfig.setMinIdle(4);
      poolConfig.setTestOnBorrow(true);
      poolConfig.setTestOnReturn(true);
      poolConfig.setTestWhileIdle(true);
      poolConfig.setMinEvictableIdleTime(Duration.ofSeconds(60));
      poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
      poolConfig.setNumTestsPerEvictionRun(3);
      poolConfig.setBlockWhenExhausted(true);
      return poolConfig;
    }

    @Override
    public void put(byte[] key, byte[] value) {
      try (Jedis jedis = jedisPool.getResource()) {
        jedis.setex(key, KEY_TIMEOUT.getSeconds(), value);
      }
    }

    @Override
    public byte[] get(byte[] key) {
      try (Jedis jedis = jedisPool.getResource()) {
        return jedis.get(key);
      }
    }
  }
}
