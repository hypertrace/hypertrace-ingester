package org.hypertrace.core.rawspansgrouper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.kafkastreams.framework.punctuators.ThrottledPunctuatorConfig;
import org.hypertrace.core.kafkastreams.framework.punctuators.action.ScheduleAction;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.spannormalizer.SpanIdentity;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.TraceState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TraceEmitCallbackRegistryTest {
  private static final long groupingWindowTimeoutMs = 300;
  private static final TraceIdentity traceIdentity =
      TraceIdentity.newBuilder()
          .setTenantId("__default")
          .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
          .build();
  private TraceEmitCallbackRegistry emitCallback;
  private KeyValueStore<SpanIdentity, RawSpan> spanStore;
  private KeyValueStore<TraceIdentity, TraceState> traceStateStore;

  @BeforeEach
  public void setUp() {
    AvroSerde avroSerde = new AvroSerde();
    ProcessorContext context = mock(ProcessorContext.class);
    when(context.keySerde()).thenReturn(avroSerde);
    spanStore = mock(KeyValueStore.class);
    traceStateStore = mock(KeyValueStore.class);
    To outputTopicProducer = mock(To.class);
    emitCallback =
        new TraceEmitCallbackRegistry(
            mock(ThrottledPunctuatorConfig.class),
            mock(KeyValueStore.class),
            context,
            spanStore,
            traceStateStore,
            outputTopicProducer,
            groupingWindowTimeoutMs,
            -1);
  }

  @Test
  public void testWhenWindowIsExtended() {
    TraceState traceState =
        TraceState.newBuilder()
            .setSpanIds(List.of(ByteBuffer.wrap("span-1".getBytes())))
            .setEmitTs(-1)
            .setTraceStartTimestamp(100)
            .setTraceEndTimestamp(200)
            .setTenantId("tenant")
            .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
            .build();
    when(traceStateStore.get(eq(traceIdentity))).thenReturn(traceState);

    ScheduleAction callbackAction = emitCallback.callback(300, traceIdentity);
    assertEquals(
        traceState.getTraceEndTimestamp() + groupingWindowTimeoutMs,
        callbackAction.getRescheduleTimestamp().get());
    // the above when() call should be the only interaction
    verify(traceStateStore, times(1)).get(traceIdentity);
  }

  @Test
  public void testWhenTraceToBeEmitted() {
    TraceState traceState =
        TraceState.newBuilder()
            .setSpanIds(List.of(ByteBuffer.wrap("span-1".getBytes())))
            .setEmitTs(-1)
            .setTraceStartTimestamp(100)
            .setTraceEndTimestamp(130)
            .setTenantId("tenant")
            .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
            .build();
    when(traceStateStore.get(eq(traceIdentity))).thenReturn(traceState);

    RawSpan rawSpan =
        RawSpan.newBuilder()
            .setCustomerId("__default")
            .setEvent(
                Event.newBuilder()
                    .setEventId(ByteBuffer.wrap("span-1".getBytes()))
                    .setCustomerId("__default")
                    .build())
            .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
            .build();
    when(spanStore.delete(any())).thenReturn(rawSpan);

    assertTrue(emitCallback.callback(450, traceIdentity).getRescheduleTimestamp().isEmpty());

    verify(traceStateStore, times(1)).get(traceIdentity);
    verify(spanStore, times(1)).delete(any());
    verify(traceStateStore).delete(eq(traceIdentity));
  }
}
