package org.hypertrace.core.rawspansgrouper;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.TraceState;

class TraceStateStoreWrapper {

  private final Cache<TraceIdentity, TraceState> cache;
  private final KeyValueStore<TraceIdentity, TraceState> traceStateStore;

  public TraceStateStoreWrapper(KeyValueStore<TraceIdentity, TraceState> traceStateStore) {
    this.cache = CacheBuilder.newBuilder().build();
    this.traceStateStore = traceStateStore;
  }

  void add(TraceIdentity traceIdentity, TraceState traceState) {
    traceStateStore.put(traceIdentity, traceState);
    cache.put(traceIdentity, traceState);
  }

  TraceState get(TraceIdentity traceIdentity) {
    return cache.getIfPresent(traceIdentity);
  }

  ConcurrentMap<TraceIdentity, TraceState> getAll() {
    return cache.asMap();
  }
}
