package org.hypertrace.semantic.convention.utils.span;

import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.spannormalizer.SpanIdentity;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;

public class SpanStore {
  private static final String SPAN_STORE_ENABLED = "span.store.enabled";
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
  private final Optional<Jedis> jedis;
  private Optional<String> bypassKey;

  public SpanStore(Config config) {
    boolean enabled =
        config.hasPath(SPAN_STORE_ENABLED) ? config.getBoolean(SPAN_STORE_ENABLED) : false;
    if (enabled) {
      DefaultJedisClientConfig defaultJedisClientConfig =
          DefaultJedisClientConfig.builder()
              .password(config.getString(SPAN_STORE_REDIS_PASSWORD))
              .build();
      jedis =
          Optional.of(
              new Jedis(
                  config.getString(SPAN_STORE_REDIS_HOST),
                  config.getInt(SPAN_STORE_REDIS_PORT),
                  defaultJedisClientConfig));
      bypassKey =
          config.hasPath(SPAN_BYPASSED_CONFIG)
              ? Optional.of(config.getString(SPAN_BYPASSED_CONFIG))
              : Optional.empty();
    } else {
      jedis = Optional.empty();
    }
  }

  public Event pushToSpanStoreAndTrimSpan(Event event) {
    try {
      if (jedis.isPresent()) {
        SpanIdentity spanIdentity =
            SpanIdentity.newBuilder()
                .setTenantId(event.getCustomerId())
                .setTraceId(TRACE_ID)
                .setSpanId(event.getEventId())
                .build();
        Event eventCopy = Event.newBuilder(event).build();
        jedis.get().set(spanIdentity.toByteBuffer().array(), eventCopy.toByteBuffer().array());
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
      }
    } catch (IOException e) {
      System.out.println(e);
    }

    return event;
  }

  public Event retrieveFromSpanStoreAndFillSpan(Event event) {
    try {
      if (jedis.isPresent()) {
        if (event
            .getEnrichedAttributes()
            .getAttributeMap()
            .getOrDefault(IS_TRIM, ATTRIBUTE_VALUE_FALSE)
            .equals(ATTRIBUTE_VALUE_TRUE)) {
          SpanIdentity spanIdentity =
              SpanIdentity.newBuilder()
                  .setTenantId(event.getCustomerId())
                  .setTraceId(TRACE_ID)
                  .setSpanId(event.getEventId())
                  .build();
          byte[] bytes = jedis.get().get(spanIdentity.toByteBuffer().array());
          if (bytes != null) {

            return Event.fromByteBuffer(ByteBuffer.wrap(bytes));
          }
        }
      }
    } catch (IOException e) {
      System.out.println(e);
    }
    return event;
  }
}
